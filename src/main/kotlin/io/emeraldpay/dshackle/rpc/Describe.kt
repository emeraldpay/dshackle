package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ConfiguredUpstreams
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class Describe(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val subscribeStatus: SubscribeStatus
) {

    fun describe(requestMono: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        return requestMono.map { _ ->
            val resp = BlockchainOuterClass.DescribeResponse.newBuilder()
            upstreams.getAvailable().forEach { chain ->
                upstreams.getUpstream(chain)?.let { chainUpstreams ->
                    chainUpstreams.getAll().let { ups ->
                        if (ups.isNotEmpty()) {
                            val status = subscribeStatus.chainStatus(chain, ups)
                            resp.addChains(
                                    BlockchainOuterClass.DescribeChain.newBuilder()
                                            .setChain(Common.ChainRef.forNumber(chain.id))
                                            .setStatus(status)
                                            .build()
                            )
                        }
                    }
                }
            }
            resp.build()
        }
    }

}