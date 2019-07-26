package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.test.MockServer
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.domain.BlockHash
import org.apache.commons.codec.binary.Hex
import spock.lang.Specification

import java.util.concurrent.CompletableFuture

class GrpcUpstreamSpec extends Specification {

    MockServer mockServer = new MockServer()
    ObjectMapper objectMapper = TestingCommons.objectMapper()

    def "Subscribe to head"() {
        setup:
        def callData = [:]
        def client = mockServer.runServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                callData.chain = request.getTypeValue()
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                            .setBlockId("50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
                            .setHeight(650246)
                            .setWeight(ByteString.copyFrom(Hex.decodeHex("35bbde5595de6456")))
                            .build()
                )
            }
        })
        def chain = Chain.ETHEREUM
        def upstream = new GrpcUpstream(chain, client, objectMapper)
        when:
        upstream.connect()
        def h = upstream.head.head.block()
        then:
        callData.chain == Chain.ETHEREUM.id
        upstream.status == UpstreamAvailability.OK
        h.hash == BlockHash.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
    }

    def "Follows difficulty, ignores less difficult"() {
        setup:
        def callData = [:]
        def finished = new CompletableFuture<Boolean>()
        def client = mockServer.runServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId("50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
                                .setHeight(650246)
                                .setWeight(ByteString.copyFrom(Hex.decodeHex("35bbde5595de6456")))
                                .build()
                )
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId("3ec2ebf5d0ec474d0ac6bca770d8409ad750d26e119968e7919f85d5ec891521")
                                .setHeight(650247)
                                .setWeight(ByteString.copyFrom(Hex.decodeHex("35bbde5595de6455")))
                                .build()
                )
                finished.complete(true)
            }
        })
        def chain = Chain.ETHEREUM
        def upstream = new GrpcUpstream(chain, client, objectMapper)
        when:
        upstream.connect()
        finished.get()
        def h = upstream.head.head.block()
        then:
        upstream.status == UpstreamAvailability.OK
        h.hash == BlockHash.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
        h.number == 650246
    }

    def "Follows difficulty"() {
        setup:
        def callData = [:]
        def finished = new CompletableFuture<Boolean>()
        def client = mockServer.runServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId("50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
                                .setHeight(650246)
                                .setWeight(ByteString.copyFrom(Hex.decodeHex("35bbde5595de6456")))
                                .build()
                )
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId("3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec891521a")
                                .setHeight(650247)
                                .setWeight(ByteString.copyFrom(Hex.decodeHex("35bbde5595de6457")))
                                .build()
                )
                finished.complete(true)
            }
        })
        def chain = Chain.ETHEREUM
        def upstream = new GrpcUpstream(chain, client, objectMapper)
        when:
        upstream.connect()
        finished.get()
        def h = upstream.head.head.block()
        then:
        upstream.status == UpstreamAvailability.OK
        h.hash == BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec891521a")
        h.number == 650247
    }
}
