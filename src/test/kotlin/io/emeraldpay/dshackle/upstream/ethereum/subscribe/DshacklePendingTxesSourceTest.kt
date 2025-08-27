package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import com.google.protobuf.ByteString
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk
import java.time.Duration

class DshacklePendingTxesSourceTest :
    ShouldSpec({

        should("Produce values") {
            var receivedRequest: BlockchainOuterClass.NativeSubscribeRequest? = null

            val uniqueName = InProcessServerBuilder.generateName()
            val server =
                InProcessServerBuilder
                    .forName(uniqueName)
                    .directExecutor()
                    .addService(
                        object : BlockchainGrpc.BlockchainImplBase() {
                            override fun nativeSubscribe(
                                request: BlockchainOuterClass.NativeSubscribeRequest,
                                responseObserver: StreamObserver<BlockchainOuterClass.NativeSubscribeReplyItem>,
                            ) {
                                receivedRequest = request
                                listOf(
                                    "\"0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c\"",
                                    "\"0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e\"",
                                    "\"0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248\"",
                                    "\"0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2\"",
                                ).map { it.toByteArray() }
                                    .map {
                                        BlockchainOuterClass.NativeSubscribeReplyItem
                                            .newBuilder()
                                            .setPayload(ByteString.copyFrom(it))
                                            .build()
                                    }.forEach {
                                        responseObserver.onNext(it)
                                    }
                                responseObserver.onCompleted()
                            }
                        },
                    ).build()
                    .start()
            val channel =
                InProcessChannelBuilder
                    .forName(uniqueName)
                    .directExecutor()
                    .build()

            val remote = ReactorBlockchainGrpc.newReactorStub(channel)
            val pending = DshacklePendingTxesSource(Chain.ETHEREUM, remote)

            pending.available = true
            val txes =
                pending
                    .connect()
                    .take(3)
                    .collectList()
                    .block(Duration.ofSeconds(1))

            receivedRequest shouldNotBe null
            receivedRequest!!.chainValue shouldBe Chain.ETHEREUM.id
            receivedRequest!!.method shouldBe "newPendingTransactions"

            txes.map { it.toHex() } shouldBe
                listOf(
                    "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                    "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e",
                    "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
                )
        }

        should("Be available when method is enabled on remote") {
            val pending = DshacklePendingTxesSource(Chain.ETHEREUM, ReactorBlockchainGrpc.newReactorStub(mockk()))
            pending.available = false

            pending.update(
                BlockchainOuterClass.DescribeChain
                    .newBuilder()
                    .addAllSupportedSubscriptions(listOf("newPendingTransactions"))
                    .build(),
            )

            pending.available shouldBe true
        }

        should("Be unavailable when not method is enabled on remote") {
            val pending = DshacklePendingTxesSource(Chain.ETHEREUM, ReactorBlockchainGrpc.newReactorStub(mockk()))
            pending.available = false

            pending.update(
                BlockchainOuterClass.DescribeChain
                    .newBuilder()
                    .addAllSupportedSubscriptions(listOf("other_method"))
                    .build(),
            )

            pending.available shouldBe false

            // now test: when it was enabled before getting an update

            pending.available = true

            pending.update(
                BlockchainOuterClass.DescribeChain
                    .newBuilder()
                    .addAllSupportedSubscriptions(listOf("other_method"))
                    .build(),
            )

            pending.available shouldBe false
        }
    })
