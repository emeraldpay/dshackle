/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.commons.DurableFlux
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.monitoring.requestlog.CurrentRequestLogWriter
import io.emeraldpay.dshackle.startup.UpstreamChange
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ForkWatchFactory
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ethereum.ConnectionMetrics
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.NettyChannelBuilder
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.netty.handler.ssl.ApplicationProtocolConfig
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import org.springframework.util.backoff.ExponentialBackOff
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.net.ConnectException
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import kotlin.concurrent.withLock

class GrpcUpstreams(
    private val id: String,
    private val forkWatchFactory: ForkWatchFactory,
    private val role: UpstreamsConfig.UpstreamRole,
    private val conn: UpstreamsConfig.GrpcConnection,
    private val fileResolver: FileResolver,
    private val currentRequestLogWriter: CurrentRequestLogWriter,
) {
    private val log = LoggerFactory.getLogger(GrpcUpstreams::class.java)

    var options = UpstreamsConfig.PartialOptions.getDefaults().build()

    private var clientValue: ReactorBlockchainGrpc.ReactorBlockchainStub? = null
    private val known = HashMap<Chain, DefaultUpstream>()
    private val lock = ReentrantLock()

    private val client: ReactorBlockchainGrpc.ReactorBlockchainStub
        get() {
            if (clientValue != null) {
                return clientValue!!
            }
            val channel: ManagedChannelBuilder<*> = if (conn.auth != null && StringUtils.isNotEmpty(conn.auth!!.ca)) {
                NettyChannelBuilder.forAddress(conn.host, conn.port)
                    // some messages are very large. many of them in megabytes, some even in gigabytes (ex. ETH Traces)
                    .maxInboundMessageSize(Int.MAX_VALUE)
                    .useTransportSecurity()
                    .enableRetry()
                    .maxRetryAttempts(3)
                    .sslContext(withTls(conn.auth!!))
            } else {
                ManagedChannelBuilder.forAddress(conn.host, conn.port)
                    .let {
                        if (conn.autoTls == true) {
                            it.useTransportSecurity()
                        } else {
                            log.warn("Using insecure connection to ${conn.host}:${conn.port}")
                            it.usePlaintext()
                        }
                    }
            }

            this.clientValue = ReactorBlockchainGrpc.newReactorStub(channel.build())
            return this.clientValue!!
        }

    fun subscribeUpstreamChanges(): Flux<UpstreamChange> {
        val connect = {
            Flux.interval(Duration.ZERO, Duration.ofMinutes(1))
                .flatMap { client.describe(BlockchainOuterClass.DescribeRequest.newBuilder().build()) }
                .transform(catchIOError())
                .flatMap(::processDescription)
                .doOnError { t -> log.error("Failed to process update from gRPC upstream $id", t) }
        }

        return DurableFlux(
            connect,
            ExponentialBackOff(100L, 1.5),
            log,
            AtomicBoolean(true)
        ).connect()
    }

    fun startStatusUpdates(): Disposable {
        val connection = DurableFlux(
            {
                client
                    .subscribeStatus(BlockchainOuterClass.StatusRequest.newBuilder().build())
                    .transform(catchIOError())
            },
            ExponentialBackOff(100L, 1.5),
            log,
            AtomicBoolean(true)
        )
        return connection
            .connect()
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe { value ->
                val chain = Chain.byId(value.chain.number)
                known[chain]?.onStatus(value)
            }
    }

    fun <T> catchIOError(): java.util.function.Function<Flux<T>, Flux<T>> {
        return java.util.function.Function<Flux<T>, Flux<T>> { source ->
            source.onErrorContinue { t, _ ->
                if (ExceptionUtils.indexOfType(t, ConnectException::class.java) >= 0) {
                    log.warn("gRPC upstream ${conn.host}:${conn.port} is unavailable. (${t.javaClass}: ${t.message})")
                    known.values.forEach {
                        it.setStatus(UpstreamAvailability.UNAVAILABLE)
                    }
                } else {
                    log.error("Failed to get description from ${conn.host}:${conn.port}", t)
                }
            }
        }
    }

    fun processDescription(value: BlockchainOuterClass.DescribeResponse): Flux<UpstreamChange> {
        val current = value.chainsList.filter {
            Chain.byId(it.chain.number) != Chain.UNSPECIFIED
        }.mapNotNull { chainDetails ->
            try {
                val chain = Chain.byId(chainDetails.chain.number)
                val up = getOrCreate(chain)
                (up.upstream as GrpcUpstream).update(chainDetails)
                up
            } catch (e: Throwable) {
                log.warn("Skip unsupported upstream ${chainDetails.chain} on $id: ${e.message}")
                null
            }
        }

        val added = current.filter {
            it.type == UpstreamChange.ChangeType.ADDED
        }

        val removed = known.filterNot { kv ->
            val stillCurrent = current.any { c -> c.chain == kv.key }
            stillCurrent
        }.map {
            UpstreamChange(it.key, known.remove(it.key)!!, UpstreamChange.ChangeType.REMOVED)
        }
        return Flux.fromIterable(removed + added)
    }

    internal fun withTls(auth: AuthConfig.ClientTlsAuth): SslContext {
        val sslContext = SslContextBuilder.forClient()
            .clientAuth(ClientAuth.REQUIRE)
        sslContext.trustManager(fileResolver.resolve(auth.ca!!).inputStream())
        if (StringUtils.isNotEmpty(auth.key) && StringUtils.isNoneEmpty(auth.certificate)) {
            sslContext.keyManager(
                fileResolver.resolve(auth.certificate!!).inputStream(),
                fileResolver.resolve(auth.key!!).inputStream()
            )
        } else {
            log.warn("Connect to remote using only CA certificate")
        }
        val alpn = ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            "grpc-exp", "h2"
        )
        sslContext.applicationProtocolConfig(alpn)
        return sslContext.build()
    }

    fun getOrCreate(chain: Chain): UpstreamChange {
        val metrics = Supplier {
            val metricsTags = listOf(
                Tag.of("upstream", id),
                Tag.of("chain", chain.chainCode)
            )

            RpcMetrics(
                metricsTags,
                timer = Timer.builder("upstream.grpc.conn")
                    .description("Request time through a Dshackle/gRPC connection")
                    .tags(metricsTags)
                    .publishPercentileHistogram()
                    .register(Metrics.globalRegistry),
                fails = Counter.builder("upstream.grpc.fail")
                    .description("Number of failures of Dshackle/gRPC requests")
                    .tags(metricsTags)
                    .register(Metrics.globalRegistry),
                responseSize = DistributionSummary.builder("upstream.grpc.response.size")
                    .description("Size of Dshackle/gRPC responses")
                    .baseUnit("Bytes")
                    .tags(metricsTags)
                    .register(Metrics.globalRegistry),
                connectionMetrics = ConnectionMetrics(metricsTags)
            )
        }

        val blockchainType = BlockchainType.from(chain)
        if (blockchainType == BlockchainType.ETHEREUM) {
            return getOrCreateEthereum(chain, metrics)
        } else if (blockchainType == BlockchainType.BITCOIN) {
            return getOrCreateBitcoin(chain, metrics)
        } else {
            throw IllegalArgumentException("Unsupported blockchain: $chain")
        }
    }

    fun getOrCreateEthereum(chain: Chain, metrics: Supplier<RpcMetrics>): UpstreamChange {
        lock.withLock {
            val current = known[chain]
            return if (current == null) {
                val rpcClient = JsonRpcGrpcClient(client, chain, metrics.get()) {
                    currentRequestLogWriter.wrap(it, id, Channel.DSHACKLE)
                }
                val created = EthereumGrpcUpstream(id, forkWatchFactory.create(chain), role, chain, this.options, client, rpcClient)
                created.timeout = this.options.timeout
                known[chain] = created
                created.start()
                UpstreamChange(chain, created, UpstreamChange.ChangeType.ADDED)
            } else {
                UpstreamChange(chain, current, UpstreamChange.ChangeType.REVALIDATED)
            }
        }
    }

    fun getOrCreateBitcoin(chain: Chain, metrics: Supplier<RpcMetrics>): UpstreamChange {
        lock.withLock {
            val current = known[chain]
            return if (current == null) {
                val rpcClient = JsonRpcGrpcClient(client, chain, metrics.get()) {
                    currentRequestLogWriter.wrap(it, id, Channel.DSHACKLE)
                }
                val created = BitcoinGrpcUpstream(id, forkWatchFactory.create(chain), role, chain, this.options, client, rpcClient)
                created.timeout = this.options.timeout
                known[chain] = created
                created.start()
                UpstreamChange(chain, created, UpstreamChange.ChangeType.ADDED)
            } else {
                UpstreamChange(chain, current, UpstreamChange.ChangeType.REVALIDATED)
            }
        }
    }

    fun get(chain: Chain): DefaultUpstream {
        return known[chain]!!
    }
}
