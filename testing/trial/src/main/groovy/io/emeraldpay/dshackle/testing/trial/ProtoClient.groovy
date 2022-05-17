package io.emeraldpay.dshackle.testing.trial

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.primitives.Bytes
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.grpc.Chain
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder

class ProtoClient implements Client {
    private int sequence = 0;
    private ObjectMapper objectMapper;
    private ReactorBlockchainGrpc.ReactorBlockchainStub stub
    private Chain chain

    ProtoClient(ManagedChannel channel, Chain chain) {
        this.stub = ReactorBlockchainGrpc.newReactorStub(channel)
        this.objectMapper = new ObjectMapper()
        this.chain= chain
    }

    static ProtoClient create(String host, int port, Chain chain) {
        def channel = NettyChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .usePlaintext()
        new ProtoClient(channel.build(), chain)
    }

    static ProtoClient basic() {
        create("localhost", 12448, Chain.ETHEREUM)
    }

    BlockchainOuterClass.NativeCallReplyItem executeNative(String method, List<Object> params, String nonce) {
        def req = BlockchainOuterClass.NativeCallRequest
                .newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                .addItems(BlockchainOuterClass.NativeCallItem
                        .newBuilder()
                        .setId(0)
                        .setNonce(nonce)
                        .setMethod(method)
                        .setPayload(ByteString.copyFrom(objectMapper.writeValueAsBytes(params)))
                        .build()
                ).build()
        stub.nativeCall(req)
                .single()
                .block()
    }

    Map<String, Object> execute(String method, List<Object> params) {
        return execute(sequence++, method, params)
    }
    Map<String, Object> execute(Object id, String method, List<Object> params) {
        def result = executeNative(method, params, "")
        if (result.errorMessage != "") {
            return [error: result.errorMessage]
        } else {
            return objectMapper.readerFor(Map).readValue(Bytes.concat("{\"result\": ".bytes, result.payload.toByteArray(), "}".bytes))
        }
    }
}