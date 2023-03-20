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

import brave.grpc.GrpcTracing
import io.emeraldpay.api.proto.BlockchainOuterClass.DescribeRequest
import io.emeraldpay.api.proto.BlockchainOuterClass.DescribeResponse
import io.emeraldpay.api.proto.BlockchainOuterClass.StatusRequest
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.FileResolver
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.grpc.Codec
import io.grpc.netty.NettyChannelBuilder
import io.micrometer.core.instrument.Counter
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
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.io.IOException
import java.time.Duration
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GrpcUpstreams(
    private val id: String,
    private val hash: Byte,
    private val role: UpstreamsConfig.UpstreamRole,
    private val host: String,
    private val port: Int,
    private val auth: AuthConfig.ClientTlsAuth? = null,
    private val compression: Boolean,
    private val fileResolver: FileResolver,
    private val nodeRating: Int,
    private val labels: UpstreamsConfig.Labels,
    private val chainStatusScheduler: Scheduler,
    private val grpcExecutor: Executor,
    private val chainsConfig: ChainsConfig,
    private val grpcTracing: GrpcTracing
) {
    private val log = LoggerFactory.getLogger(GrpcUpstreams::class.java)

    var timeout = Defaults.timeout

    lateinit var client: ReactorBlockchainGrpc.ReactorBlockchainStub
    private val known = HashMap<Chain, DefaultUpstream>()
    private val lock = ReentrantLock()

    fun start(): Flux<UpstreamChangeEvent> {
        val chanelBuilder = NettyChannelBuilder.forAddress(host, port)
            // some messages are very large. many of them in megabytes, some even in gigabytes (ex. ETH Traces)
            .maxInboundMessageSize(Defaults.maxMessageSize)
            .enableRetry()
            .intercept(grpcTracing.newClientInterceptor())
            .executor(grpcExecutor)
            .maxRetryAttempts(3)
        if (auth != null && StringUtils.isNotEmpty(auth.ca)) {
            chanelBuilder
                .useTransportSecurity()
                .sslContext(withTls(auth))
        } else {
            log.warn("Using insecure connection to $host:$port")
            chanelBuilder.usePlaintext()
        }

        var client = ReactorBlockchainGrpc.newReactorStub(chanelBuilder.build())
        if (compression) {
            client = client.withCompression(Codec.Gzip().messageEncoding)
        }
        this.client = client

        val statusSubscription = AtomicReference<Disposable>()

        return Flux.interval(Duration.ZERO, Duration.ofSeconds(20))
            .flatMap {
                client.describe(DescribeRequest.newBuilder().build())
            }.onErrorContinue { t, _ ->
                if (ExceptionUtils.indexOfType(t, IOException::class.java) >= 0) {
                    log.warn("gRPC upstream $host:$port is unavailable. (${t.javaClass}: ${t.message})")
                    known.values.forEach {
                        it.setStatus(UpstreamAvailability.UNAVAILABLE)
                    }
                } else {
                    log.error("Failed to get description from $host:$port", t)
                }
            }.flatMap { value ->
                processDescription(value)
            }.doOnNext {
                val subscription = client.subscribeStatus(
                    StatusRequest.newBuilder()
                        .addChains(Common.ChainRef.forNumber(it.chain.id)).build()
                ).subscribeOn(chainStatusScheduler)
                    .subscribe { value ->
                        val chain = Chain.byId(value.chain.number)
                        if (chain != Chain.UNSPECIFIED) {
                            known[chain]?.onStatus(value)
                        }
                    }
                statusSubscription.updateAndGet { prev ->
                    prev?.dispose()
                    subscription
                }
            }.doOnError { t ->
                log.error("Failed to process update from gRPC upstream $id", t)
            }
    }

    private fun processDescription(value: DescribeResponse): Flux<UpstreamChangeEvent> {
        val chainNames = value.chainsList.map { it.chain.name }
        val version = value.buildInfo.version
        log.info("Start processing grpc upstream description for $id with chains $chainNames and version $version")
        val current = value.chainsList.filter {
            Chain.byId(it.chain.number) != Chain.UNSPECIFIED
        }.mapNotNull { chainDetails ->
            try {
                val chain = Chain.byId(chainDetails.chain.number)
                val up = getOrCreate(chain)
                val changed = (up.upstream as GrpcUpstream).update(chainDetails, value.buildInfo)
                up.takeUnless {
                    changed && it.type == UpstreamChangeEvent.ChangeType.REVALIDATED
                } ?: UpstreamChangeEvent(up.chain, up.upstream, UpstreamChangeEvent.ChangeType.UPDATED)
            } catch (e: Throwable) {
                log.warn("Skip unsupported upstream ${chainDetails.chain} on $id: ${e.message}")
                null
            }
        }

        val added = current.filter {
            it.type == UpstreamChangeEvent.ChangeType.ADDED
        }

        val updated = current.filter {
            it.type == UpstreamChangeEvent.ChangeType.UPDATED
        }

        val removed = known.filterNot { kv ->
            val stillCurrent = current.any { c -> c.chain == kv.key }
            stillCurrent
        }.map {
            UpstreamChangeEvent(it.key, known.remove(it.key)!!, UpstreamChangeEvent.ChangeType.REMOVED)
        }
        if (removed.isNotEmpty() || added.isNotEmpty() || updated.isNotEmpty()) {
            log.info("Finished processing of grpc upstream description for $id with content delta added ${added.map { it.chain }}, updated ${updated.map { it.chain }} and removed ${removed.map { it.chain }}")
        }
        return Flux.fromIterable(removed + added + updated)
    }

    private fun withTls(auth: AuthConfig.ClientTlsAuth): SslContext {
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

    private val creators: Map<BlockchainType, (chain: Chain, client: JsonRpcGrpcClient) -> DefaultUpstream> = mapOf(
        BlockchainType.EVM_POW to { chain, rpcClient ->
            EthereumGrpcUpstream(id, hash, role, chain, client, rpcClient, labels, chainsConfig.resolve(chain))
        },
        BlockchainType.EVM_POS to { chain, rpcClient ->
            EthereumPosGrpcUpstream(id, hash, role, chain, client, rpcClient, nodeRating, labels, chainsConfig.resolve(chain))
        },
        BlockchainType.BITCOIN to { chain, rpcClient ->
            BitcoinGrpcUpstream(id, role, chain, client, rpcClient, labels, chainsConfig.resolve(chain))
        }
    )

    private fun getOrCreate(chain: Chain): UpstreamChangeEvent {
        val metrics = makeMetrics(chain)
        val creator = creators.getValue(BlockchainType.from(chain))
        return getOrCreate(chain, metrics, creator)
    }

    private fun makeMetrics(chain: Chain): RpcMetrics {
        val metricsTags = listOf(
            Tag.of("upstream", id),
            Tag.of("chain", chain.chainCode)
        )

        return RpcMetrics(
            Timer.builder("upstream.grpc.conn")
                .description("Request time through a Dshackle/gRPC connection")
                .tags(metricsTags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry),
            Counter.builder("upstream.grpc.fail")
                .description("Number of failures of Dshackle/gRPC requests")
                .tags(metricsTags)
                .register(Metrics.globalRegistry)
        )
    }

    private fun getOrCreate(
        chain: Chain,
        metrics: RpcMetrics,
        creator: (chain: Chain, client: JsonRpcGrpcClient) -> DefaultUpstream
    ): UpstreamChangeEvent {
        lock.withLock {
            val current = known[chain]
            return if (current == null) {
                val rpcClient = JsonRpcGrpcClient(client, chain, metrics)
                val created = creator(chain, rpcClient)
                known[chain] = created
                if (created is Lifecycle) created.start()
                UpstreamChangeEvent(chain, created, UpstreamChangeEvent.ChangeType.ADDED)
            } else {
                UpstreamChangeEvent(chain, current, UpstreamChangeEvent.ChangeType.REVALIDATED)
            }
        }
    }
}
