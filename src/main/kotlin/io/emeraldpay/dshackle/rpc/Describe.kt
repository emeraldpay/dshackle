package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ConfiguredUpstreams
import io.emeraldpay.dshackle.upstream.Upstreams
import io.grpc.stub.StreamObserver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class Describe(
        @Autowired private val upstreams: Upstreams
) {

    fun describe(request: BlockchainOuterClass.DescribeRequest, responseObserver: StreamObserver<BlockchainOuterClass.DescribeResponse>) {
        val resp = BlockchainOuterClass.DescribeResponse.newBuilder()
        upstreams.getAvailable().forEach { chain ->
            upstreams.ethereumUpstream(chain).let { chainUpstreams ->
                val quorum = chainUpstreams.getAll().map { u ->
                    if (u.getStatus() == UpstreamAvailability.OK) {
                        u.getOptions().quorum
                    } else {
                        0
                    }
                }.sum()
                val available = chainUpstreams.getAll().any { u ->
                    u.getStatus() == UpstreamAvailability.OK
                }
                resp.addChains(
                    BlockchainOuterClass.DescribeChain.newBuilder()
                            .setChain(Common.ChainRef.forNumber(chain.id))
                            .setQuorum(quorum)
                            .setAvailable(available)
                            .build()
                )
            }
        }
        responseObserver.onNext(resp.build())
        responseObserver.onCompleted()
    }
}