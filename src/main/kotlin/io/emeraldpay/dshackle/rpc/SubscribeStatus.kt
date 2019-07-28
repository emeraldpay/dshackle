package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.Disposable

@Service
class SubscribeStatus(
        @Autowired private val upstreams: Upstreams
) {

    fun subscribeStatus(request: BlockchainOuterClass.StatusRequest, responseObserver: StreamObserver<BlockchainOuterClass.ChainStatus>) {
        upstreams.getAvailable().forEach { chain ->
            var d: Disposable? = null
            val chainUpstream = upstreams.getUpstream(chain)
            d = chainUpstream?.observeStatus()?.subscribe { availability ->
                val status = chainStatus(chain, chainUpstream.getAll())
                try {
                    responseObserver.onNext(status)
                } catch (e: StatusRuntimeException) {
                    // gRPC channel was closed
                    d?.dispose()
                }
            }
        }
    }

    fun chainStatus(chain: Chain, ups: List<Upstream>): BlockchainOuterClass.ChainStatus {
        val available = ups.map { u ->
            u.getStatus()
        }.min()!!
        val quorum = ups.filter {
            it.getStatus() > UpstreamAvailability.UNAVAILABLE
        }.count()
        val status = BlockchainOuterClass.ChainStatus.newBuilder()
                .setAvailability(BlockchainOuterClass.AvailabilityEnum.forNumber(available.grpcId))
                .setChain(Common.ChainRef.forNumber(chain.id))
                .setQuorum(quorum)
                .build()
        return status
    }

}