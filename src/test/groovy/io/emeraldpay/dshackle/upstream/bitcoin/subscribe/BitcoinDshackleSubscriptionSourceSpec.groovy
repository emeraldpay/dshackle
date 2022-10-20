package io.emeraldpay.dshackle.upstream.bitcoin.subscribe

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.test.MockGrpcServer
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import spock.lang.Specification

import java.time.Duration

class BitcoinDshackleSubscriptionSourceSpec extends Specification {

    MockGrpcServer mockServer = new MockGrpcServer()

    def "correct upstream decoding"() {
        setup:
        def blockchainStub = mockServer.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void nativeSubscribe(BlockchainOuterClass.NativeSubscribeRequest request, StreamObserver<BlockchainOuterClass.NativeSubscribeReplyItem> responseObserver) {
                responseObserver.onNext(
                        BlockchainOuterClass.NativeSubscribeReplyItem.newBuilder()
                                .setPayload(ByteString.copyFrom( [ 1, 2, 3, 4, 5, 6, 7 ] as byte[]))
                                .build()
                )
            }
        })

        def subscription = new BitcoinDshackleSubscriptionSource(Chain.BITCOIN, blockchainStub, BitcoinZmqTopic.HASHBLOCK)
        when:
        def act = subscription.connect()
                .take(1)
                .collectList()
                .block(Duration.ofSeconds(3))

        then:
        act == [
                [ 1, 2, 3, 4, 5, 6, 7 ] as byte[],
        ]
    }
}
