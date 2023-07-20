package io.emeraldpay.dshackle.upstream.rpcclient

import com.google.protobuf.ByteString
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.test.MockGrpcServer
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.grpc.stub.StreamObserver
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.instanceOf
import reactor.core.Exceptions
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class JsonRpcGrpcClientTest : ShouldSpec({

    should("Make a request") {
        val mockGrpc = MockGrpcServer()
        val requested = AtomicReference<BlockchainOuterClass.NativeCallRequest>()

        val gprc = mockGrpc.clientForServer(object : BlockchainGrpc.BlockchainImplBase() {
            override fun nativeCall(request: BlockchainOuterClass.NativeCallRequest, responseObserver: StreamObserver<BlockchainOuterClass.NativeCallReplyItem>) {
                requested.set(request)
                responseObserver.onNext(
                    BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setId(1)
                        .setSucceed(true)
                        .setPayload(ByteString.copyFromUtf8("\"hello world!\""))
                        .build()
                )
                responseObserver.onCompleted()
            }
        })
        val client = JsonRpcGrpcClient(
            gprc, Chain.BITCOIN, null, null
        ).forSelector("test", Selector.empty)

        val act = client.read(JsonRpcRequest("test", emptyList()))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act!!.hasError shouldBe false
        act.resultAsProcessedString shouldBe "hello world!"

        requested.get() shouldBe BlockchainOuterClass.NativeCallRequest.newBuilder()
            .setChain(Common.ChainRef.CHAIN_BITCOIN)
            .addAllItems(
                listOf(
                    BlockchainOuterClass.NativeCallItem.newBuilder()
                        .setId(1)
                        .setMethod("test")
                        .setPayload(ByteString.copyFromUtf8("[]"))
                        .build()
                )
            )
            .build()
    }

    should("Return error on HTTP error") {
        val mockGrpc = MockGrpcServer()

        val gprc = mockGrpc.clientForServer(object : BlockchainGrpc.BlockchainImplBase() {
            override fun nativeCall(request: BlockchainOuterClass.NativeCallRequest, responseObserver: StreamObserver<BlockchainOuterClass.NativeCallReplyItem>) {
                responseObserver.onError(IllegalStateException("fail"))
            }
        })

        val client = JsonRpcGrpcClient(
            gprc, Chain.BITCOIN, null, null
        ).forSelector("test", Selector.empty)

        val act = shouldThrowAny {
            client.read(JsonRpcRequest("test", emptyList()))
                .block(Duration.ofSeconds(1))
        }.let(Exceptions::unwrap)

        act shouldBe instanceOf<RpcException>()
        with((act as RpcException).error) {
            message shouldBe "Remote status code: UNKNOWN"
            code shouldBe RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR
        }
    }

    should("Process original JSON RPC Error") {
        val mockGrpc = MockGrpcServer()

        val gprc = mockGrpc.clientForServer(object : BlockchainGrpc.BlockchainImplBase() {
            override fun nativeCall(request: BlockchainOuterClass.NativeCallRequest, responseObserver: StreamObserver<BlockchainOuterClass.NativeCallReplyItem>) {
                responseObserver.onNext(
                    BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setId(1)
                        .setSucceed(false)
                        .setPayload(ByteString.copyFromUtf8("{\"code\":-32123,\"message\":\"test error\",\"details\":null}"))
                        .setErrorMessage("test error")
                        .build()
                )
                responseObserver.onCompleted()
            }
        })

        val client = JsonRpcGrpcClient(
            gprc, Chain.ETHEREUM, null, null
        ).forSelector("test", Selector.empty)

        val act = client.read(JsonRpcRequest("test", emptyList()))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act!!.hasError shouldBe true
        act.error shouldNotBe null
        with(act.error!!) {
            code shouldBe -32123
            message shouldBe "test error"
            details shouldBe null
        }
        act.hasResult shouldBe false
    }
})
