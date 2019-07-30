package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.NettyChannelBuilder
import io.netty.handler.ssl.*
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.io.File
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GrpcUpstreams(
        private val host: String,
        private val port: Int,
        private val objectMapper: ObjectMapper,
        private val options: UpstreamsConfig.Options,
        private val auth: UpstreamsConfig.TlsAuth? = null,
        private val availableChains: AvailableChains
) {
    private val log = LoggerFactory.getLogger(GrpcUpstreams::class.java)

    private var client: ReactorBlockchainGrpc.ReactorBlockchainStub? = null
    private var known = HashMap<Chain, GrpcUpstream>()
    private val lock = ReentrantLock()

    fun start(): Mono<List<Chain>> {
        val channel: ManagedChannelBuilder<*> = if (auth != null && StringUtils.isNotEmpty(auth.ca)) {
            NettyChannelBuilder.forAddress(host, port)
                    .useTransportSecurity()
                    .sslContext(withTls(auth))
        } else {
            log.warn("Using insecure connection for $host:$port")
            ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
        }

        val client = ReactorBlockchainGrpc.newReactorStub(channel.build())
        this.client = client
        val loaded = client.describe(BlockchainOuterClass.DescribeRequest.newBuilder().build())
                .map { value ->
                    val chains = ArrayList<Chain>()
                    value.chainsList.forEach { chainDetails ->
                        val chain = Chain.byId(chainDetails.chain.number)
                        if (chain != Chain.UNSPECIFIED) {
                            getOrCreate(chain)
                                    .init(chainDetails)
                            chains.add(chain)
                        }
                    }
                    chains as List<Chain>
                }
                .doOnError { t ->
                    log.error("Failed to get description from $host:$port", t)
                }
        //TODO subscribe only after receiving details
        client.subscribeStatus(BlockchainOuterClass.StatusRequest.newBuilder().build())
                .subscribe { value ->
                    val chain = Chain.byId(value.chain.number)
                    if (chain != Chain.UNSPECIFIED) {
                        known[chain]?.onStatus(value)
                    }
                }
        return loaded
    }

    internal fun withTls(auth: UpstreamsConfig.TlsAuth): SslContext {
        val sslContext = SslContextBuilder.forClient()
                .clientAuth(ClientAuth.REQUIRE)
        sslContext.trustManager(File(auth.ca!!).inputStream())
        if (StringUtils.isNotEmpty(auth.key) && StringUtils.isNoneEmpty(auth.certificate)) {
            sslContext.keyManager(File(auth.certificate!!).inputStream(), File(auth.key!!).inputStream())
        } else {
            log.warn("Connect to remote using only CA certificate")
        }
        val alpn = ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                "grpc-exp", "h2")
        sslContext.applicationProtocolConfig(alpn)
        return sslContext.build()
    }

    fun getOrCreate(chain: Chain): GrpcUpstream {
        lock.withLock {
            val current = known[chain]
            return if (current == null) {
                val created = GrpcUpstream(chain, client!!, objectMapper, options)
                known[chain] = created
                availableChains.add(chain)
                created.connect()
                created
            } else {
                current
            }
        }
    }

}