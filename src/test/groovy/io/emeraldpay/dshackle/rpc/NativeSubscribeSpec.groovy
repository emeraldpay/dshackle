/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosMultiStream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumSubscribe
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class NativeSubscribeSpec extends Specification {

    def "Call with empty params when not provided"() {
        setup:
        def subscribe = Mock(EthereumSubscribe) {
            1 * it.subscribe("newHeads", null, _ as Selector.AnyLabelMatcher) >> Flux.just("{}")
        }
        def up = Mock(EthereumPosMultiStream) {
            1 * it.getSubscribe() >> subscribe
        }

        def nativeSubscribe = new NativeSubscribe(new MultistreamHolderMock(Chain.ETHEREUM, up))
        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM.id)
                .setMethod("newHeads")
                .build()
        when:
        def act = nativeSubscribe.start(call)

        then:
        StepVerifier.create(act)
                .expectNext("{}")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Call with params when provided"() {
        setup:
        def subscribe = Mock(EthereumSubscribe) {
            1 * it.subscribe("logs", { params ->
                println("params: $params")
                def ok = params instanceof Map &&
                        params["address"] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" &&
                        params["topics"] instanceof List &&
                        params["topics"][0] == "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
                println("ok: $ok")
                ok
            }, _ as Selector.AnyLabelMatcher) >> Flux.just("{}")
        }
        def up = Mock(EthereumPosMultiStream) {
            1 * it.getSubscribe() >> subscribe
        }

        def nativeSubscribe = new NativeSubscribe(new MultistreamHolderMock(Chain.ETHEREUM, up))
        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM.id)
                .setMethod("logs")
                .setPayload(ByteString.copyFromUtf8(
                        '{"address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", ' +
                                '"topics": ["0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"]}'
                ))
                .build()
        when:
        def act = nativeSubscribe.start(call)

        then:
        StepVerifier.create(act)
                .expectNext("{}")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
