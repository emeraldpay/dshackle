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
package io.emeraldpay.dshackle.rpc


import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.upstream.CachingEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.quorum.NonEmptyQuorum
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.RpcResponseException
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.TimeoutException

class NativeCallSpec extends Specification {

    def objectMapper = TestingCommons.objectMapper()

    def "Quorum is applied"() {
        setup:
        def quorum = Spy(new AlwaysQuorum())
        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answer("eth_test", [], "foo")

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock),
                Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(2))
        def act = objectMapper.readValue(resp.payload, Map)
        then:
        act == [jsonrpc:"2.0", id:1, result: "foo"]
        1 * quorum.record(_, _)
        1 * quorum.getResult()
    }

    def "Quorum may return not first received value"() {
        setup:
        def quorum = Spy(new NonEmptyQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answerOnce("eth_test", [], null)
        apiMock.answerOnce("eth_test", [], "bar")
        apiMock.answerOnce("eth_test", [], null)

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock),
                Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))


        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(2))
        def act = objectMapper.readValue(resp.payload, Map)
        then:
        act == [jsonrpc:"2.0", id:1, result: "bar"]
        2 * quorum.record(_, _)
        1 * quorum.getResult()
    }

    def "Have pause between repeats"() {
        setup:
        def quorum = Spy(new NonEmptyQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answerOnce("eth_test", [], null)
        apiMock.answerOnce("eth_test", [], "bar")

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock),
                Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))


        when:
        def t1 = System.currentTimeMillis()
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(2))
        def delta = System.currentTimeMillis() - t1
        then:
        delta > 95 // should be 100, but sometimes gives less ???
        new String(resp.payload) == '{"jsonrpc":"2.0","id":1,"result":"bar"}'
    }

    def "One call has no pause"() {
        setup:
        def quorum = Spy(new NonEmptyQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answerOnce("eth_test", [], "bar")

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock),
                Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))


        when:
        def t1 = System.currentTimeMillis()
        nativeCall.executeOnRemote(call).block(Duration.ofSeconds(2))
        def delta = System.currentTimeMillis() - t1
        then:
        delta < 50
    }

    def "Returns error if no quorum"() {
        setup:
        def quorum = Spy(new NonEmptyQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answer("eth_test", [], null, 3)
        apiMock.answerOnce("eth_test", [], "foo")

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        3 * quorum.record(_, _)
        1 * quorum.getResult()

        when:
        def resp = nativeCall.executeOnRemote(call)
        then:
        StepVerifier.create(resp)
            .expectErrorMatches({t -> t instanceof NativeCall.CallFailure && t.id == 1})
            .verify(Duration.ofSeconds(1))
    }

    def "Packs call exception into response with id"() {
        setup:
        def upstreams = Stub(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        when:
        def resp = nativeCall.processException(new NativeCall.CallFailure(5, new IllegalArgumentException("test test")))
        then:
        StepVerifier.create(resp)
                .expectNext(BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setSucceed(false)
                        .setErrorMessage("Failed to call 5: test test")
                        .setId(5)
                        .build())
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Packs unknown exception into response"() {
        setup:
        def upstreams = Stub(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        when:
        def resp = nativeCall.processException(new IllegalArgumentException("test test"))
        then:
        StepVerifier.create(resp)
                .expectNext(BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setSucceed(false)
                        .setErrorMessage("test test")
                        .build())
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Builds normal response"() {
        setup:
        def upstreams = Stub(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def json = [jsonrpc:"2.0", id:1, result: "foo"]

        when:
        def resp = nativeCall.buildResponse(
                new NativeCall.CallContext<byte[]>(1561, TestingCommons.aggregatedUpstream(Stub(DirectEthereumApi)), Selector.empty, new AlwaysQuorum(), objectMapper.writeValueAsBytes(json))
        )
        then:
        resp.id == 1561
        resp.succeed
        objectMapper.readValue(resp.payload.toByteArray(), Map.class) == [jsonrpc:"2.0", id:1, result: "foo"]
    }

    def "Returns error for invalid chain"() {
        setup:
        def upstreams = Stub(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
            .setChainValue(0)
            .addAllItems([1, 2].collect { id ->
                return BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(id)
                .setMethod("eth_test")
                .build()
            })
            .build()
        when:
        def resp = nativeCall.prepareCall(req)
        then:
        StepVerifier.create(resp)
                .expectErrorMatches({t -> t instanceof NativeCall.CallFailure && t.id == 0})
//                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Returns error for unsupported chain"() {
        setup:
        def upstreams =  Mock(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChainValue(Chain.TESTNET_MORDEN.id)
                .addAllItems([1, 2].collect { id ->
                    return BlockchainOuterClass.NativeCallItem.newBuilder()
                            .setId(id)
                            .setMethod("eth_test")
                            .build()
                })
                .build()
        1 * upstreams.isAvailable(Chain.TESTNET_MORDEN) >> false
        when:
        def resp = nativeCall.prepareCall(req)
        then:
        StepVerifier.create(resp)
                .expectErrorMatches({t -> t instanceof NativeCall.CallFailure && t.id == 0})
//                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Calls cache before remote"() {
        setup:
        def upstreams = Stub(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def api = Mock(DirectEthereumApi)
        def upstream = TestingCommons.aggregatedUpstream(api)
        def cacheMock = Mock(CachingEthereumApi)
        upstream.cache = cacheMock

        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(10,
                upstream,
                Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []))
        when:
        nativeCall.fetch(ctx)
        then:
        1 * cacheMock.execute(10, "eth_test", []) >> Mono.empty()
    }

    def "Uses cached value"() {
        setup:
        def upstreams = Stub(Upstreams)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def upstream = TestingCommons.aggregatedUpstream(Stub(DirectEthereumApi))
        def cacheMock = Mock(CachingEthereumApi)
        upstream.cache = cacheMock

        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(10,
                upstream,
                Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []))
        when:
        def act = nativeCall.fetch(ctx)
        then:
        1 * cacheMock.execute(10, "eth_test", []) >> Mono.just('{"result": "foo"}'.bytes)
        new String(act.block().payload) == '{"result": "foo"}'
    }

    def "Retries on error"() {
        setup:
        def quorum = Spy(new AlwaysQuorum())

        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answer("eth_test", [], null, 1, new TimeoutException("test 1"))
        apiMock.answer("eth_test", [], null, 1, new TimeoutException("test 2"))
        apiMock.answerOnce("eth_test", [], "bar")

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock),
                Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))


        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(2))
        def act = objectMapper.readValue(resp.payload, Map)
        then:
        act == [jsonrpc:"2.0", id:1, result: "bar"]
        1 * quorum.record(_, _)
        1 * quorum.getResult()
    }

    def "Send raw retries 3 times"() {
        setup:
        def quorum = Spy(new BroadcastQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answer("eth_sendRawTransaction", ["0x1234"],
                "0x4b66b555df9faed6f0711f2104d183736c8e2dc7434626dd2622e243f041d41b", 1)
        apiMock.answer("eth_sendRawTransaction", ["0x1234"], null, 10,
                new RpcException(RpcResponseError.CODE_INVALID_REQUEST, "Transaction with the same hash was already imported"))
//        apiMock.answer("eth_sendRawTransaction", ["0x1234"],
//                new RpcResponseError(RpcResponseError.CODE_INVALID_REQUEST, "Transaction with the same hash was already imported"), 10)

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock),
                Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_sendRawTransaction", ["0x1234"]))


        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(2))
        def act = objectMapper.readValue(resp.payload, Map)
        then:
        act == [jsonrpc:"2.0", id:1, result: "0x4b66b555df9faed6f0711f2104d183736c8e2dc7434626dd2622e243f041d41b"]
        1 * quorum.record(_ as byte[], _)
        2 * quorum.record(_ as RpcException, _)
    }
}
