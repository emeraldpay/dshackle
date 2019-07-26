package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import io.grpc.ManagedChannelBuilder
import reactor.core.publisher.Mono
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GrpcUpstreams(
        private val host: String,
        private val port: Int,
        private val objectMapper: ObjectMapper,
        private val options: UpstreamsConfig.Options
) {

    private var client: ReactorBlockchainGrpc.ReactorBlockchainStub? = null
    private var known = HashMap<Chain, GrpcUpstream>()
    private val lock = ReentrantLock()

    fun start(): Mono<List<Chain>> {
        val channel = ManagedChannelBuilder.forAddress(host, port)
                .enableRetry()
        channel.usePlaintext()
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
        //TODO subscribe only after receiving details
        client.subscribeStatus(BlockchainOuterClass.StatusRequest.newBuilder().build())
                .subscribe { value ->
                    val chain = Chain.byId(value.chain.number)
                    if (chain != Chain.UNSPECIFIED) {
                        getOrCreate(chain).onStatus(value)
                    }
                }
        return loaded
    }

    fun getOrCreate(chain: Chain): GrpcUpstream {
        lock.withLock {
            val current = known[chain]
            return if (current == null) {
                val created = GrpcUpstream(chain, client!!, objectMapper, options)
                known[chain] = created
                created.connect()
                created
            } else {
                current
            }
        }
    }

}