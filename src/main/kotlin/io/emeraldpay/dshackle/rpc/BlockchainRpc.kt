package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.domain.TransactionId
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class BlockchainRpc(
        @Autowired private val nativeCall: NativeCall,
        @Autowired private val streamHead: StreamHead,
        @Autowired private val trackTx: TrackTx
): BlockchainGrpc.BlockchainImplBase() {

    override fun nativeCall(request: BlockchainOuterClass.CallBlockchainRequest, responseObserver: StreamObserver<BlockchainOuterClass.CallBlockchainReplyItem>) {
        nativeCall.nativeCall(request, responseObserver)
    }

    override fun streamHead(request: Common.Chain, responseObserver: StreamObserver<BlockchainOuterClass.ChainHead>) {
        streamHead.add(Chain.byId(request.type.number), responseObserver)
    }

    override fun trackTx(request: BlockchainOuterClass.TrackTxRequest, responseObserver: StreamObserver<BlockchainOuterClass.TxStatus>) {
        val tx = TrackTx.TrackedTx(
                Chain.byId(request.chainValue),
                StreamSender(responseObserver),
                Instant.now(),
                TransactionId.from(request.txid),
                Math.min(Math.max(1, request.confirmations), 100)
        )
        trackTx.add(tx)
    }
}