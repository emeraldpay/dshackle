/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.test.EthereumApiMock
import io.emeraldpay.dshackle.test.MockServer
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.RpcCall
import io.infinitape.etherjar.rpc.RpcClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

class EthereumGrpcTransportSpec extends Specification {

    MockServer mockServer = new MockServer()
    ObjectMapper objectMapper = TestingCommons.objectMapper()
    def ethereumTargets = new QuorumBasedMethods(objectMapper, Chain.ETHEREUM)

    def "Make simple call"() {
        setup:
        def otherSideApi = new EthereumApiMock(Mock(RpcClient), objectMapper, Chain.ETHEREUM)

        def callData = [:]
        def otherSideUpstreams = Mock(Upstreams)
        def otherSideAggr = TestingCommons.aggregatedUpstream(otherSideApi)

        def otherSideNativeCall = new NativeCall(otherSideUpstreams, objectMapper)
        otherSideApi.upstream = otherSideAggr

        def client = mockServer.clientForServer(new ReactorBlockchainGrpc.BlockchainImplBase() {
            @Override
            Flux<BlockchainOuterClass.NativeCallReplyItem> nativeCall(Mono<BlockchainOuterClass.NativeCallRequest> request) {
                callData["request"] = request.block()
                return otherSideNativeCall.nativeCall(request)
            }
        })

        EthereumGrpcTransport transport = new EthereumGrpcTransport(Chain.ETHEREUM, client, objectMapper)
        when:
        otherSideApi.answer("eth_test", [1], "bar")
        def batch = new Batch()
        def f = batch.add(RpcCall.create("eth_test", [1]))
        def status = transport.execute(batch.items).get()

        then:
        1 * otherSideUpstreams.getUpstream(Chain.ETHEREUM) >> otherSideAggr
        status.failed == 0
        status.succeed == 1
        status.total == 1
        callData.request != null
        with((BlockchainOuterClass.NativeCallRequest)callData.request) {
            chain.number == Chain.ETHEREUM.id
            itemsCount == 1
            with(getItems(0)) {
                method == "eth_test"
                payload.toStringUtf8() == "[1]"
            }
        }
        f.get() == "bar"
    }

    def "Make few calls"() {
        setup:
        def otherSideApi = new EthereumApiMock(Mock(RpcClient), objectMapper, Chain.ETHEREUM)

        def callData = [:]
        def otherSideUpstreams = Mock(Upstreams)
        def otherSideAggr = TestingCommons.aggregatedUpstream(otherSideApi)
        def otherSideNativeCall = new NativeCall(otherSideUpstreams, objectMapper)
        otherSideApi.upstream = otherSideAggr

        def client = mockServer.clientForServer(new ReactorBlockchainGrpc.BlockchainImplBase() {
            @Override
            Flux<BlockchainOuterClass.NativeCallReplyItem> nativeCall(Mono<BlockchainOuterClass.NativeCallRequest> request) {
                callData["request"] = request.block()
                return otherSideNativeCall.nativeCall(request)
            }

        })

        EthereumGrpcTransport transport = new EthereumGrpcTransport(Chain.ETHEREUM, client, objectMapper)
        when:
        otherSideApi.answer("eth_test", [1], "bar")
        otherSideApi.answer("eth_test2", [2, "3"], "baz")

        def batch = new Batch()
        def f1 = batch.add(RpcCall.create("eth_test", [1]))
        def f2 = batch.add(RpcCall.create("eth_test2", [2, "3"]))
        def status = transport.execute(batch.items).get()

        then:
        1 * otherSideUpstreams.getUpstream(Chain.ETHEREUM) >> otherSideAggr
        status.failed == 0
        status.succeed == 2
        status.total == 2
        callData.request != null
        with((BlockchainOuterClass.NativeCallRequest)callData.request) {
            chain.number == Chain.ETHEREUM.id
            itemsCount == 2
            with(getItems(0)) {
                method == "eth_test"
                payload.toStringUtf8() == "[1]"
            }
            with(getItems(1)) {
                method == "eth_test2"
                payload.toStringUtf8() == "[2,\"3\"]"
            }
        }
        f1.get() == "bar"
        f2.get() == "baz"
    }

}
