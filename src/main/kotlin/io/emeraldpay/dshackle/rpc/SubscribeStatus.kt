package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.Upstreams
import io.grpc.stub.StreamObserver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class SubscribeStatus(
        @Autowired private val upstreams: Upstreams
) {

    fun subscribeStatus(request: BlockchainOuterClass.StatusRequest, responseObserver: StreamObserver<BlockchainOuterClass.ChainStatus>) {
        upstreams.getAvailable().forEach { chain ->
        }
    }

}