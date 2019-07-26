package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.domain.TransactionId
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class BlockchainRpc(
        @Autowired private val nativeCall: NativeCall,
        @Autowired private val streamHead: StreamHead,
        @Autowired private val trackTx: TrackTx,
        @Autowired private val trackAddress: TrackAddress,
        @Autowired private val describe: Describe,
        @Autowired private val subscribeStatus: SubscribeStatus
): BlockchainGrpc.BlockchainImplBase() {

    private val log = LoggerFactory.getLogger(BlockchainRpc::class.java)

    override fun nativeCall(request: BlockchainOuterClass.NativeCallRequest, responseObserver: StreamObserver<BlockchainOuterClass.NativeCallReplyItem>) {
        nativeCall.nativeCall(request, responseObserver)
    }

    override fun subscribeHead(request: Common.Chain, responseObserver: StreamObserver<BlockchainOuterClass.ChainHead>) {
        streamHead.add(Chain.byId(request.type.number), responseObserver)
    }

    override fun subscribeTxStatus(request: BlockchainOuterClass.TxStatusRequest, responseObserver: StreamObserver<BlockchainOuterClass.TxStatus>) {
        val tx = TrackTx.TrackedTx(
                Chain.byId(request.chainValue),
                StreamSender(responseObserver),
                Instant.now(),
                TransactionId.from(request.txId),
                Math.min(Math.max(1, request.confirmationLimit), 100)
        )
        trackTx.add(tx)
    }

    override fun subscribeBalance(request: BlockchainOuterClass.BalanceRequest, responseObserver: StreamObserver<BlockchainOuterClass.AddressBalance>) {
        trackAddress.add(request, responseObserver)
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest, responseObserver: StreamObserver<BlockchainOuterClass.AddressBalance>) {
        val addresses = trackAddress.initializeFor(request, responseObserver)
        trackAddress.send(request, addresses)
                .doOnError { t ->
                    log.error("Failed to process balance", t)
                    responseObserver.onError(Exception("Internal error"))
                }
                .subscribe {
                    responseObserver.onCompleted()
                }

    }

    override fun describe(request: BlockchainOuterClass.DescribeRequest, responseObserver: StreamObserver<BlockchainOuterClass.DescribeResponse>) {
        describe.describe(request, responseObserver)
    }

    override fun subscribeStatus(request: BlockchainOuterClass.StatusRequest, responseObserver: StreamObserver<BlockchainOuterClass.ChainStatus>) {
        subscribeStatus.subscribeStatus(request, responseObserver)
    }
}