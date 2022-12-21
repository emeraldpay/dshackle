/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainGrpc
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.Selector
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import spock.lang.Specification

import java.time.Duration

class DshacklePendingTxesSourceSpec extends Specification {

    def "Produces values"() {
        setup:
        BlockchainOuterClass.NativeSubscribeRequest receivedRequest

        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor()
                .addService(new BlockchainGrpc.BlockchainImplBase() {
                    @Override
                    void nativeSubscribe(BlockchainOuterClass.NativeSubscribeRequest request, StreamObserver<BlockchainOuterClass.NativeSubscribeReplyItem> responseObserver) {
                        receivedRequest = request
                        [
                                '"0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c"',
                                '"0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e"',
                                '"0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248"',
                                '"0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2"'
                        ]
                                    .collect { it.bytes }
                                    .collect {
                                        BlockchainOuterClass.NativeSubscribeReplyItem.newBuilder()
                                            .setPayload(ByteString.copyFrom(it))
                                            .build()
                                    }.forEach {
                                        responseObserver.onNext(it)
                                    }
                        responseObserver.onCompleted()
                    }
                })
                .build().start()
        ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build()

        def remote = ReactorBlockchainGrpc.newReactorStub(channel)
        def pending = new DshacklePendingTxesSource(Chain.ETHEREUM, remote)

        when:
        pending.available = true
        def txes = pending.connect(Selector.empty).take(3)
                .collectList().block(Duration.ofSeconds(1))

        then:
        receivedRequest != null
        receivedRequest.chainValue == Chain.ETHEREUM.id
        receivedRequest.method == "newPendingTransactions"
        txes.collect {it.toHex() } == [
                "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e",
                "0x9a9d4618b12d36d17a63d48c5b5efc05b461feead124ddd86803d8bca4015248",
        ]
    }

    def "available when method is enabled on remote"() {
        setup:
        def pending = new DshacklePendingTxesSource(Chain.ETHEREUM, ReactorBlockchainGrpc.newReactorStub(Stub(Channel)))
        pending.available = false
        when:
        pending.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                .addAllSupportedMethods(["newPendingTransactions"])
                .build()
        )
        then:
        pending.available
    }

    def "unavailable when not method is enabled on remote"() {
        setup:
        def pending = new DshacklePendingTxesSource(Chain.ETHEREUM, ReactorBlockchainGrpc.newReactorStub(Stub(Channel)))
        pending.available = false
        when:
        pending.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addAllSupportedMethods(["other_method"])
                        .build()
        )
        then:
        !pending.available

        when: "It was enabled before getting an update"
        pending.available = true
        pending.update(
                BlockchainOuterClass.DescribeChain.newBuilder()
                        .addAllSupportedMethods(["other_method"])
                        .build()
        )
        then:
        !pending.available
    }
}
