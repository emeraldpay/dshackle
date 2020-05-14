/**
 * Copyright (c) 2019 ETCDEV GmbH
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.grpc

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.test.MockGrpcServer
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcGrpcClient
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.emerald.ReactorEmeraldClient
import io.infinitape.etherjar.rpc.json.BlockJson
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture

class EthereumGrpcUpstreamSpec extends Specification {

    MockGrpcServer mockServer = new MockGrpcServer()
    ObjectMapper objectMapper = TestingCommons.objectMapper()

    def "Subscribe to head"() {
        setup:
        def callData = [:]
        def chain = Chain.ETHEREUM
        def api = TestingCommons.api()
        def block1 = new BlockJson().with {
            it.number = 650246
            it.hash = BlockHash.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
            it.totalDifficulty = new BigInteger("35bbde5595de6456", 16)
            it.timestamp = Instant.now()
            return it
        }
        api.answer("eth_getBlockByHash", [block1.hash.toHex(), false], block1)
        def client = mockServer.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
                api.nativeCall(request, responseObserver)
            }

            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                callData.chain = request.getTypeValue()
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId(block1.hash.toHex().substring(2))
                                .setHeight(block1.number)
                                .setWeight(ByteString.copyFrom(block1.totalDifficulty.toByteArray()))
                                .build()
                )
            }
        })
        def upstream = new EthereumGrpcUpstream("test", chain, client, objectMapper, new JsonRpcGrpcClient(client, chain, objectMapper))
        upstream.setLag(0)
        upstream.init(BlockchainOuterClass.DescribeChain.newBuilder()
                .addAllSupportedMethods(["eth_getBlockByHash"])
                .build())
        when:
        upstream.start()
        def h = upstream.head.getFlux().next().block(Duration.ofSeconds(1))
        then:
        callData.chain == Chain.ETHEREUM.id
        upstream.status == UpstreamAvailability.OK
        h.hash == BlockId.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
    }

    def "Follows difficulty, ignores less difficult"() {
        setup:
        def api = TestingCommons.api()
        def block1 = new BlockJson().with {
            it.number = 650246
            it.hash = BlockHash.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
            it.totalDifficulty = new BigInteger("35bbde5595de6456", 16)
            it.timestamp = Instant.now()
            return it
        }
        def block2 = new BlockJson().with {
            it.number = 650247
            it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec891521a")
            it.totalDifficulty = new BigInteger("35bbde5595de6455", 16)
            it.timestamp = Instant.now()
            return it
        }
        api.answer("eth_getBlockByHash", [block1.hash.toHex(), false], block1)
        api.answer("eth_getBlockByHash", [block2.hash.toHex(), false], block2)
        def client = mockServer.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
                api.nativeCall(request, responseObserver)
            }

            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId(block1.hash.toHex().substring(2))
                                .setHeight(block1.number)
                                .setWeight(ByteString.copyFrom(block1.totalDifficulty.toByteArray()))
                                .build()
                )
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId(block2.hash.toHex().substring(2))
                                .setHeight(block2.number)
                                .setWeight(ByteString.copyFrom(block2.totalDifficulty.toByteArray()))
                                .build()
                )
            }
        })
        def upstream = new EthereumGrpcUpstream("test", Chain.ETHEREUM, client, objectMapper, new JsonRpcGrpcClient(client, Chain.ETHEREUM, objectMapper))
        upstream.setLag(0)
        upstream.init(BlockchainOuterClass.DescribeChain.newBuilder()
                .addAllSupportedMethods(["eth_getBlockByHash"])
                .build())
        when:
        upstream.start()
        def h = upstream.head.getFlux().take(Duration.ofSeconds(1)).last().block()
        then:
        upstream.status == UpstreamAvailability.OK
        h.hash == BlockId.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
        h.height == 650246
    }

    def "Follows difficulty"() {
        setup:
        def callData = [:]
        def finished = new CompletableFuture<Boolean>()
        def chain = Chain.ETHEREUM
        def api = TestingCommons.api()
        def block1 = new BlockJson().with {
            it.number = 650246
            it.hash = BlockHash.from("0x50d26e119968e791970d84a7bf5d0ec474d3ec2ef85d5ec8915210ac6bc09ad7")
            it.totalDifficulty = new BigInteger("35bbde5595de6456", 16)
            it.timestamp = Instant.now()
            return it
        }
        def block2 = new BlockJson().with {
            it.number = 650247
            it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec891521a")
            it.totalDifficulty = new BigInteger("35bbde5595de6457", 16)
            it.timestamp = Instant.now()
            return it
        }
        api.answer("eth_getBlockByHash", [block1.hash.toHex(), false], block1)
        api.answer("eth_getBlockByHash", [block2.hash.toHex(), false], block2)
        def client = mockServer.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void nativeCall(BlockchainOuterClass.NativeCallRequest request, StreamObserver<BlockchainOuterClass.NativeCallReplyItem> responseObserver) {
                api.nativeCall(request, responseObserver)
            }

            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId(block1.hash.toHex().substring(2))
                                .setHeight(block1.number)
                                .setWeight(ByteString.copyFrom(block1.totalDifficulty.toByteArray()))
                                .build()
                )
                responseObserver.onNext(
                        BlockchainOuterClass.ChainHead.newBuilder()
                                .setBlockId(block2.hash.toHex().substring(2))
                                .setHeight(block2.number)
                                .setWeight(ByteString.copyFrom(block2.totalDifficulty.toByteArray()))
                                .build()
                )
                finished.complete(true)
            }
        })
        def upstream = new EthereumGrpcUpstream("test", chain, client, objectMapper, new JsonRpcGrpcClient(client, chain, objectMapper))
        upstream.setLag(0)
        upstream.init(BlockchainOuterClass.DescribeChain.newBuilder()
                .addAllSupportedMethods(["eth_getBlockByHash"])
                .build())
        when:
        upstream.start()
        finished.get()
        def h = upstream.head.getFlux().take(Duration.ofSeconds(1)).last().block()
        then:
        upstream.status == UpstreamAvailability.OK
        h.hash == BlockId.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec891521a")
        h.height == 650247
    }
}
