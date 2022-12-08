package io.emeraldpay.dshackle.upstream.rpcclient

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.test.MockGrpcServer
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.dshackle.Chain
import io.grpc.stub.StreamObserver
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class JsonRpcGrpcClientSpec extends Specification {

    def "Makes a request"() {
        setup:
        def mockGrpc = new MockGrpcServer()
        def requested = new AtomicReference<BlockchainOuterClass.NativeCallRequest>()

        def grpc = mockGrpc.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
                requested.set(request)
                responseObserver.onNext(BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setId(1)
                        .setSucceed(true)
                        .setPayload(ByteString.copyFromUtf8("\"hello world!\""))
                        .build())
                responseObserver.onCompleted()
            }
        })
        def client = new JsonRpcGrpcClient(
                grpc, Chain.BITCOIN, null
        ).getReader()

        when:
        def act = client.read(
                new JsonRpcRequest("test", [])
        ).block(Duration.ofSeconds(1))

        then:
        !act.hasError()
        act.resultAsProcessedString == "hello world!"

        requested.get() == BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_BITCOIN)
                .addAllItems([
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("test")
                                .setPayload(ByteString.copyFromUtf8("[]"))
                                .build()
                ])
                .build()
    }

    def "Return error on HTTP error"() {
        setup:
        def mockGrpc = new MockGrpcServer()
        def grpc = mockGrpc.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
                responseObserver.onError(new IllegalStateException("fail"))
            }
        })
        def client = new JsonRpcGrpcClient(
                grpc, Chain.BITCOIN, null
        ).getReader()

        when:
        client.read(
                new JsonRpcRequest("test", [])
        ).block(Duration.ofSeconds(1))

        then:
        def t = thrown(RpcException)
        with(t.error) {
            message == "Remote status code: UNKNOWN"
            it.code == RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR
        }

    }
}
