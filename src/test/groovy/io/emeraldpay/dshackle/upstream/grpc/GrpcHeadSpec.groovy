/**
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

import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.test.MockGrpcServer
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.util.function.Function

class GrpcHeadSpec extends Specification {

    MockGrpcServer mockServer = new MockGrpcServer()

    def "Subscribe to remote"() {
        setup:
        def client = mockServer.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                if (request.type.number != Chain.BITCOIN.id) {
                    responseObserver.onError(new IllegalStateException("Unsupported chain"))
                    return
                }
                [10, 11, 12, 14].forEach { height ->
                    responseObserver.onNext(
                            BlockchainOuterClass.ChainHead.newBuilder()
                                    .setChain(request.type)
                                    .setHeight(height)
                                    .build()
                    )
                    Thread.sleep(100)
                }
                responseObserver.onCompleted()
            }
        })
        def convert = { BlockchainOuterClass.ChainHead head ->
            TestingCommons.blockForBitcoin(head.height)
        }
        def head = new GrpcHead(
                Chain.BITCOIN,
                Stub(DefaultUpstream),
                convert, null
        )
        when:
        def act = head.getFlux()
                .take(3)

        then:
        StepVerifier.create(act)
                .then { head.start(client) }
                .expectNext(TestingCommons.blockForBitcoin(10)).as("block 10")
                .expectNext(TestingCommons.blockForBitcoin(11)).as("block 11")
                .expectNext(TestingCommons.blockForBitcoin(12)).as("block 12")
                .expectComplete()
                .verify(Duration.ofSeconds(5))
    }

    def "Reconnect to remote on error"() {
        setup:
        int phase = 0
        def client = mockServer.clientForServer(new BlockchainGrpc.BlockchainImplBase() {
            @Override
            void subscribeHead(Common.Chain request, StreamObserver<BlockchainOuterClass.ChainHead> responseObserver) {
                if (request.type.number != Chain.BITCOIN.id) {
                    responseObserver.onError(new IllegalStateException("Unsupported chain"))
                    return
                }
                phase++
                if (phase == 1) {
                    responseObserver.onError(new RuntimeException("Phase 1 error"))
                } else if (phase == 2) {
                    [10, 11].forEach { height ->
                        responseObserver.onNext(
                                BlockchainOuterClass.ChainHead.newBuilder()
                                        .setChain(request.type)
                                        .setHeight(height)
                                        .build()
                        )
                        Thread.sleep(100)
                    }
                    responseObserver.onError(new RuntimeException("Phase 2 error"))
                } else if (phase == 3) {
                    [11, 12, 13].forEach { height ->
                        responseObserver.onNext(
                                BlockchainOuterClass.ChainHead.newBuilder()
                                        .setChain(request.type)
                                        .setHeight(height)
                                        .build()
                        )
                        Thread.sleep(100)
                    }
                    responseObserver.onCompleted()
                }
            }
        })
        def convert = { BlockchainOuterClass.ChainHead head ->
            TestingCommons.blockForBitcoin(head.height)
        }
        def head = new GrpcHead(
                Chain.BITCOIN,
                Stub(DefaultUpstream),
                convert, null
        )
        when:
        def act = head.getFlux()
                .take(3)
        head.start(client)

        then:
        StepVerifier.create(act)
                .expectNext(TestingCommons.blockForBitcoin(10))
                .expectNext(TestingCommons.blockForBitcoin(11))
                .expectNext(TestingCommons.blockForBitcoin(12))
                .expectComplete()
                .verify(Duration.ofSeconds(5))
    }

}
