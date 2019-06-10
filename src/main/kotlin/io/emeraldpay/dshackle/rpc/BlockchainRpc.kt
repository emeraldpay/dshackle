package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class BlockchainRpc(
        @Autowired private val nativeCall: NativeCall,
        @Autowired private val streamHead: StreamHead
): BlockchainGrpc.BlockchainImplBase() {

    override fun nativeCall(request: BlockchainOuterClass.CallBlockchainRequest, responseObserver: StreamObserver<BlockchainOuterClass.CallBlockchainReplyItem>) {
        nativeCall.nativeCall(request, responseObserver)
    }

    override fun streamHead(request: Common.Chain, responseObserver: StreamObserver<BlockchainOuterClass.ChainHead>) {
        streamHead.add(Chain.byId(request.type.number), responseObserver)
    }
}