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
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumEgressSubscription
import io.emeraldpay.dshackle.upstream.generic.GenericMultistream
import io.emeraldpay.dshackle.upstream.signature.NoSigner
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class NativeSubscribeSpec extends Specification {
    def signer = new NoSigner()

    def "Call with empty params when not provided"() {
        setup:
        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM__MAINNET.id)
                .setMethod("newHeads")
                .build()

        def subscribe = Mock(EthereumEgressSubscription) {
            1 * it.subscribe("newHeads", null, _ as Selector.AnyLabelMatcher) >> Flux.just("{}")
            1 * it.getAvailableTopics() >> ["newHeads"]
        }
        def up = Mock(GenericMultistream) {
            1 * it.tryProxySubscribe(_ as Selector.AnyLabelMatcher, call) >> null
            2 * it.getEgressSubscription() >> subscribe
        }

        def nativeSubscribe = new NativeSubscribe(new MultistreamHolderMock(Chain.ETHEREUM__MAINNET, up), signer)

        when:
        def act = nativeSubscribe.start(call)

        then:
        StepVerifier.create(act)
                .expectNext(new NativeSubscribe.ResponseHolder("{}", null))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Call with params when provided"() {
        setup:
        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM__MAINNET.id)
                .setMethod("logs")
                .setPayload(ByteString.copyFromUtf8(
                        '{"address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", ' +
                                '"topics": ["0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"]}'
                ))
                .build()

        def subscribe = Mock(EthereumEgressSubscription) {
            1 * it.subscribe("logs", { params ->
                println("params: $params")
                def ok = params instanceof Map &&
                        params["address"] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" &&
                        params["topics"] instanceof List &&
                        params["topics"][0] == "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
                println("ok: $ok")
                ok
            }, _ as Selector.AnyLabelMatcher) >> Flux.just("{}")
            1 * it.getAvailableTopics() >> ["logs"]
        }
        def up = Mock(GenericMultistream) {
            1 * it.tryProxySubscribe(_ as Selector.AnyLabelMatcher, call) >> null
            2 * it.getEgressSubscription() >> subscribe
        }

        def nativeSubscribe = new NativeSubscribe(new MultistreamHolderMock(Chain.ETHEREUM__MAINNET, up), signer)

        when:
        def act = nativeSubscribe.start(call)

        then:
        StepVerifier.create(act)
                .expectNext(new NativeSubscribe.ResponseHolder("{}", null))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Proxy call"() {
        setup:
        def call = BlockchainOuterClass.NativeSubscribeRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM__MAINNET.id)
                .setMethod("newHeads")
                .build()

        def subscribe = Mock(EthereumEgressSubscription) {
            1 * it.getAvailableTopics() >> ["newHeads"]
        }

        def up = Mock(GenericMultistream) {
            1 * it.tryProxySubscribe(_ as Selector.AnyLabelMatcher, call) >> Flux.just("{}")
            1 * it.getEgressSubscription() >> subscribe
        }

        def nativeSubscribe = new NativeSubscribe(new MultistreamHolderMock(Chain.ETHEREUM__MAINNET, up), signer)

        when:
        def act = nativeSubscribe.start(call)

        then:
        StepVerifier.create(act)
                .expectNext(new NativeSubscribe.ResponseHolder("{}", null))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}

