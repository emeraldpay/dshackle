package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.Upstreams
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
            d = upstreams.getUpstream(chain)?.observeStatus()?.subscribe { availability ->
                val chainStatus = BlockchainOuterClass.ChainStatus.newBuilder()
                        .setChain(Common.ChainRef.forNumber(chain.id))
                        .setAvailable(availability == UpstreamAvailability.OK)
                        .setQuorum(0)
                if (availability == UpstreamAvailability.OK) {
                    upstreams.getUpstream(chain)?.getOptions()?.let { opts ->
                        chainStatus.setQuorum(opts.quorum)
                    }
                }
                try {
                    responseObserver.onNext(chainStatus.build())
                } catch (e: StatusRuntimeException) {
                    // gRPC channel was closed
                    d?.dispose()
                }
            }
        }
    }

}