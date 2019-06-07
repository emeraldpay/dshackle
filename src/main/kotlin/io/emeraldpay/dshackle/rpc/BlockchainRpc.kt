package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.grpc.stub.StreamObserver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class BlockchainRpc(
        @Autowired private val nativeCall: NativeCall
): BlockchainGrpc.BlockchainImplBase() {

    override fun nativeCall(request: BlockchainOuterClass.CallBlockchainRequest, responseObserver: StreamObserver<BlockchainOuterClass.CallBlockchainReplyItem>) {
        nativeCall.nativeCall(request, responseObserver)
    }
}