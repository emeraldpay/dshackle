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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Ignore
import spock.lang.Specification

import java.time.Duration

class NativeCallSpec extends Specification {

    ObjectMapper objectMapper = Global.objectMapper

    def "Tries router first"() {
        def routedApi = Mock(Reader) {
            1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.just(new JsonRpcResponse("1".bytes, null))
        }
        def upstream = Mock(Multistream) {
            1 * getRoutedApi(_) >> Mono.just(routedApi)
        }
        def upstreams = Stub(MultistreamHolder)

        def nativeCall = new NativeCall(upstreams)
        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(
                1, upstream, Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", [])
        )

        when:
        def act = nativeCall.fetch(ctx).block(Duration.ofSeconds(1))
        then:
        act.result == "1".bytes
    }

    def "Return error if router denied the requests"() {
        def routedApi = Mock(Reader) {
            1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.error(new RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Test message"))
        }
        def upstream = Mock(Multistream) {
            1 * getRoutedApi(_) >> Mono.just(routedApi)
        }
        def upstreams = Stub(MultistreamHolder)

        def nativeCall = new NativeCall(upstreams)
        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(
                15, upstream, Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", [])
        )

        when:
        def act = nativeCall.fetch(ctx) //.block(Duration.ofSeconds(1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { result ->
                    result.id == 15 && result.isError()
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Quorum is applied"() {
        setup:
        def quorum = new AlwaysQuorum()

        def nativeCall = new NativeCall(Stub(MultistreamHolder))
        nativeCall.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(_) >> Mono.just(new QuorumRpcReader.Result("\"foo\"".bytes, 1))
            }
        }
        def call = new NativeCall.CallContext(1, TestingCommons.multistream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(1))
        def act = objectMapper.readValue(resp.result, Object)
        then:
        act == "foo"
    }

    def "Returns error if no quorum"() {
        setup:
        def quorum = new AlwaysQuorum()

        def nativeCall = new NativeCall(Stub(MultistreamHolder))
        nativeCall.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(_) >> Mono.empty()
            }
        }
        def call = new NativeCall.CallContext(1, TestingCommons.multistream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        when:
        def resp = nativeCall.executeOnRemote(call)
        then:
        StepVerifier.create(resp)
                .expectNextMatches { result ->
                    result.isError()
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Packs call exception into response with id"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams)
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
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams)
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
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams)
        def json = [jsonrpc:"2.0", id:1, result: "foo"]

        when:
        def resp = nativeCall.buildResponse(
                new NativeCall.CallResult(1561, objectMapper.writeValueAsBytes(json), null)
        )
        then:
        resp.id == 1561
        resp.succeed
        objectMapper.readValue(resp.payload.toByteArray(), Map.class) == [jsonrpc:"2.0", id:1, result: "foo"]
    }

    def "Returns error for invalid chain"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams)

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
        def upstreams = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = new NativeCall(upstreams)

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
                .expectErrorMatches({ t -> t instanceof NativeCall.CallFailure && t.id == 0 })
//                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Prepare call"() {
        setup:
        def upstreams = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = new NativeCall(upstreams)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("eth_test")
                                .setPayload(ByteString.copyFromUtf8("[]"))
                )
                .build()
        when:
        def act = nativeCall.prepareCall(req, TestingCommons.emptyMultistream())
                .collectList().block(Duration.ofSeconds(1))
        then:
        act.size() == 1
        with(act[0]) {
            id == 1
            payload.method == "eth_test"
            payload.params == "[]"
        }
    }

    def "Prepare call without payload"() {
        setup:
        def upstreams = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = new NativeCall(upstreams)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("eth_test")
                )
                .build()
        when:
        def act = nativeCall.prepareCall(req, TestingCommons.emptyMultistream())
                .collectList().block(Duration.ofSeconds(1))
        then:
        act.size() == 1
        with(act[0]) {
            id == 1
            payload.method == "eth_test"
            payload.params == ""
        }
    }

    def "Prepare call adds height selector for not-lagging quorum"() {
        setup:
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM),
                ["foo_bar"] as Set, [] as Set
        )
        methods.setQuorum("foo_bar", "not_lagging")
        def head = Mock(Head) {
            1 * it.getCurrentHeight() >> 101
        }
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM, TestingCommons.upstream())
        multistream.customMethods = methods
        multistream.customHead = head
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = new NativeCall(multistreamHolder)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("foo_bar")
                )
                .build()
        when:
        def act = nativeCall.prepareCall(req, multistream)
                .collectList().block(Duration.ofSeconds(1)).first()
        then:
        act.matcher != null
        act.matcher instanceof Selector.MultiMatcher
        with((Selector.MultiMatcher) act.matcher) {
            it.getMatchers().size() >= 1
            it.getMatcher(Selector.HeightMatcher) != null
            with(it.getMatcher(Selector.HeightMatcher)) {
                it.height == 101
            }
        }
    }

    def "Parse empty params"() {
        setup:
        def nativeCall = new NativeCall(Stub(MultistreamHolder))
        def ctx = new NativeCall.CallContext(1, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", "[]"))
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == []
        act.payload.method == "eth_test"
    }

    def "Parse none params"() {
        setup:
        def nativeCall = new NativeCall(Stub(MultistreamHolder))
        def ctx = new NativeCall.CallContext(1, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", ""))
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == []
        act.payload.method == "eth_test"
    }

    def "Parse single param"() {
        setup:
        def nativeCall = new NativeCall(Stub(MultistreamHolder))
        def ctx = new NativeCall.CallContext(1, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", "[false]"))
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == [false]
        act.payload.method == "eth_test"
    }

    def "Parse multi param"() {
        setup:
        def nativeCall = new NativeCall(Stub(MultistreamHolder))
        def ctx = new NativeCall.CallContext(1, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", "[false, 123]"))
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == [false, 123]
        act.payload.method == "eth_test"
    }

    @Ignore
    //TODO
    def "Calls cache before remote"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams)
        def api = TestingCommons.api()
        def upstream = TestingCommons.multistream(api)

        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(10,
                upstream,
                Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []))
        when:
        nativeCall.fetch(ctx)
        then:
        1 * cacheMock.execute(10, "eth_test", []) >> Mono.empty()
    }

    @Ignore
    //TODO
    def "Uses cached value"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams)
        def upstream = TestingCommons.multistream(TestingCommons.api())

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
}
