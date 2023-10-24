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
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.CacheConfig
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.reader.RpcReader
import io.emeraldpay.dshackle.reader.RpcReaderFactory
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Ignore
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class NativeCallSpec extends Specification {

    ObjectMapper objectMapper = Global.objectMapper

    def nativeCall(MultistreamHolder upstreams = null, ResponseSigner signer = null, Boolean enableCache = true, Boolean passthrough = false) {

        if (upstreams == null) {
            upstreams = Stub(MultistreamHolder)
        }
        if (signer == null) {
            signer = Stub(ResponseSigner)
        }

        def config = new MainConfig()
        def cacheConfig = new CacheConfig()
        cacheConfig.requestsCacheEnabled = enableCache
        config.cache = cacheConfig
        config.passthrough = passthrough

        new NativeCall(upstreams, signer, config, Stub(Tracer))
    }

    def "Tries router first"() {
        def routedApi = Mock(Reader) {
            1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.just(new JsonRpcResponse("1".bytes, null))
        }
        def upstream = Mock(Multistream) {
            1 * getLocalReader(_) >> Mono.just(routedApi)
        }

        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext<NativeCall.ParsedCallDetails>(
                1, null, upstream, Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1
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
            1 * getLocalReader(_) >> Mono.just(routedApi)
        }

        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext<NativeCall.ParsedCallDetails>(
                15, null, upstream, Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1
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

        def ups = Mock(Upstream) {
            _ * nodeId() >> (byte) 1
        }

        def nativeCall = nativeCall()
        nativeCall.rpcReaderFactory = Mock(RpcReaderFactory) {
            1 * create(_) >> Mock(RpcReader) {
                1 * read(_) >> Mono.just(new RpcReader.Result("\"foo\"".bytes, null, 1, ups))
            }
        }
        def call = new NativeCall.ValidCallContext(1, 10, TestingCommons.multistream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1)

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(1))
        def act = objectMapper.readValue(resp.result, Object)
        then:
        act == "foo"
        resp.nonce == 10
    }

    def "Returns error if no quorum"() {
        setup:
        def quorum = new AlwaysQuorum()

        def nativeCall = nativeCall()
        nativeCall.rpcReaderFactory = Mock(RpcReaderFactory) {
            1 * create(_) >> Mock(RpcReader) {
                1 * attempts() >> new AtomicInteger(1)
                1 * read(new JsonRpcRequest("eth_test", [], 10)) >> Mono.empty()
            }
        }
        def call = new NativeCall.ValidCallContext(1, 10, TestingCommons.multistream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1)

        when:
        def resp = nativeCall.executeOnRemote(call)
        then:
        StepVerifier.create(resp)
                .expectNextMatches { result ->
                    result.isError() && result.nonce == 10
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Returns error details from remote"() {
        setup:
        def quorum = new AlwaysQuorum()

        def nativeCall = nativeCall()
        nativeCall.rpcReaderFactory = Mock(RpcReaderFactory) {
            1 * create(_) >> Mock(RpcReader) {
                1 * read(new JsonRpcRequest("eth_test", [], 10)) >> Mono.error(
                        new JsonRpcException(JsonRpcResponse.Id.from(12), new JsonRpcError(-32123, "Foo Bar", "Foo Bar Baz"), null, true, null)
                )
            }
        }
        def call = new NativeCall.ValidCallContext(12, 10, TestingCommons.multistream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1)

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(1))
        then:
        resp.isError()
        with(resp.getError()) {
            message == "Foo Bar"
            upstreamError != null
            with (upstreamError) {
                code == -32123
                details == "Foo Bar Baz"
            }
        }
    }

    def "Packs call exception into response with id"() {
        setup:
        def nativeCall = nativeCall()
        when:
        def resp = nativeCall.processException(new NativeCall.CallFailure(5, new IllegalArgumentException("test test")))
        then:
        StepVerifier.create(resp)
                .expectNext(BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setSucceed(false)
                        .setErrorMessage("Failed to call 5: test test")
                        .setId(5)
                        .setErrorCode(500)
                        .build())
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Packs unknown exception into response"() {
        setup:
        def nativeCall = nativeCall()
        when:
        def resp = nativeCall.processException(new IllegalArgumentException("test test"))
        then:
        StepVerifier.create(resp)
                .expectNext(BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                        .setSucceed(false)
                        .setErrorMessage("test test")
                        .setErrorCode(500)
                        .build())
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Builds normal response"() {
        setup:
        def nativeCall = nativeCall()
        def json = [jsonrpc:"2.0", id:1, result: "foo"]

        when:
        def resp = nativeCall.buildResponse(
                new NativeCall.CallResult(1561, 10, objectMapper.writeValueAsBytes(json), null, null, null, null)
        )
        then:
        resp.id == 1561
        resp.succeed
        objectMapper.readValue(resp.payload.toByteArray(), Map.class) == [jsonrpc:"2.0", id:1, result: "foo"]
    }

    def "Builds response with signature"() {
        setup:
        def nativeCall = nativeCall()
        def json = [jsonrpc:"2.0", id:1, result: "foo"]

        when:
        def resp = nativeCall.buildResponse(
                new NativeCall.CallResult(1561, 10, objectMapper.writeValueAsBytes(json), null, new ResponseSigner.Signature("sig1".bytes, "test", 100), "test", null)
        )
        then:
        resp.id == 1561
        resp.succeed
        resp.signature.nonce == 10
        resp.signature.signature.toByteArray() == "sig1".bytes
        resp.signature.keyId == 100
        resp.signature.upstreamId == "test"
        resp.upstreamId == "test"
        objectMapper.readValue(resp.payload.toByteArray(), Map.class) == [jsonrpc:"2.0", id:1, result: "foo"]
    }

    def "Returns error for invalid chain"() {
        setup:
        def nativeCall = nativeCall()

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
                .verify(Duration.ofSeconds(1))
    }

    def "Returns error for unsupported chain"() {
        setup:
        def upstreams = Mock(MultistreamHolder)
        def nativeCall = nativeCall(upstreams)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChainValue(Chain.ETHEREUM__GOERLI.id)
                .addAllItems([1, 2].collect { id ->
                    return BlockchainOuterClass.NativeCallItem.newBuilder()
                            .setId(id)
                            .setMethod("eth_test")
                            .build()
                })
                .build()
        1 * upstreams.isAvailable(Chain.ETHEREUM__GOERLI) >> false
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
        def nativeCall = nativeCall(upstreams)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setNonce(10)
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
            nonce == 10
            payload.method == "eth_test"
            payload.params == "[]"
        }
    }

    def "Prepare call without payload"() {
        setup:
        def upstreams = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(upstreams)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
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

    def "Prepare call with unsupported method"() {
        setup:
        def upstreams = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(upstreams)

        def item = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(1)
                .setMethod("eth_testInvalid")
                .build()
        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                .addItems(item)
                .build()
        when:
        def act = nativeCall.prepareIndividualCall(Chain.ETHEREUM__MAINNET, req, item, TestingCommons.emptyMultistream())
                .block(Duration.ofSeconds(1))
        then:
        act instanceof NativeCall.InvalidCallContext
        with(((NativeCall.InvalidCallContext) act).error) {
            it.upstreamError != null
            it.upstreamError.code == -32601
            it.upstreamError.message.contains("eth_testInvalid")
        }
    }

    def "Prepare call adds height selector for not-lagging quorum"() {
        setup:
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET),
                ["foo_bar"] as Set, [] as Set, [] as Set, [] as Set
        )
        methods.setQuorum("foo_bar", "not_lagging")
        def head = Mock(Head) {
            1 * it.getCurrentHeight() >> 101
        }
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.upstream())
        multistream.customMethods = methods
        multistream.customHead = head
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(multistreamHolder)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
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

    def "Prepare call adds decorator for eth_newFilter"() {
        setup:
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET),
                ["eth_newFilter"] as Set, [] as Set, [] as Set, [] as Set
        )
        methods.setQuorum("eth_newFilter", "always")
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.upstream())
        multistream.customMethods = methods
        multistream.customHead = Mock(Head)
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(multistreamHolder)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("eth_newFilter")
                )
                .build()
        when:
        def act = nativeCall.prepareCall(req, multistream)
                .collectList().block(Duration.ofSeconds(1)).first()
        then:
        act instanceof NativeCall.ValidCallContext
        act.resultDecorator instanceof NativeCall.CreateFilterDecorator
    }

    def "Prepare call adds decorator for eth_getFilterChanges"() {
        setup:
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET),
                ["eth_getFilterChanges"] as Set, [] as Set, [] as Set, [] as Set
        )
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.upstream())
        multistream.customMethods = methods
        multistream.customHead = Mock(Head)
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(multistreamHolder)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("eth_getFilterChanges")
                )
                .build()
        when:
        def act = nativeCall.prepareCall(req, multistream)
                .collectList().block(Duration.ofSeconds(1)).first()
        then:
        act instanceof NativeCall.ValidCallContext
        act.requestDecorator instanceof NativeCall.WithFilterIdDecorator
    }

    def "Prepare call adds decorator for eth_uninstallFilter"() {
        setup:
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET),
                ["eth_uninstallFilter"] as Set, [] as Set, [] as Set, [] as Set
        )
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.upstream())
        multistream.customMethods = methods
        multistream.customHead = Mock(Head)
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(multistreamHolder)

        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                .addItems(
                        BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(1)
                                .setMethod("eth_uninstallFilter")
                )
                .build()
        when:
        def act = nativeCall.prepareCall(req, multistream)
                .collectList().block(Duration.ofSeconds(1)).first()
        then:
        act instanceof NativeCall.ValidCallContext
        act.requestDecorator instanceof NativeCall.WithFilterIdDecorator
    }

    def "Parse empty params"() {
        setup:
        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext(1, null, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", "[]"), "reqId", 1)
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == []
        act.payload.method == "eth_test"
    }

    def "Parse none params"() {
        setup:
        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext(1, null, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", ""), "reqId", 1)
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == []
        act.payload.method == "eth_test"
    }

    def "Parse single param"() {
        setup:
        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext(1, null, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", "[false]"), "reqId", 1)
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == [false]
        act.payload.method == "eth_test"
    }

    def "Parse multi param"() {
        setup:
        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext(1, null, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_test", "[false, 123]"), "reqId", 1)
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == [false, 123]
        act.payload.method == "eth_test"
    }

    def "Decorate eth_getFilterUpdates params"() {
        setup:
        def nativeCall = nativeCall()
        def ctx = new NativeCall.ValidCallContext(1, null, Stub(Multistream), Selector.empty, new AlwaysQuorum(),
                new NativeCall.RawCallDetails("eth_getFilterUpdates", '["0xabcd"]'),
                new NativeCall.WithFilterIdDecorator(), new NativeCall.NoneResultDecorator(), null, "reqId", 1)
        when:
        def act = nativeCall.parseParams(ctx)
        then:
        act.id == 1
        act.payload.params == ["0xab"]
        act.payload.method == "eth_getFilterUpdates"
    }

    def "Decorate eth_newFilter result"() {
        setup:
        def ups = Mock(Upstream) {
            _ * nodeId() >> (byte)255
        }
        def quorum = new AlwaysQuorum()
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET),
                [] as Set, [] as Set, ["filter"] as Set, [] as Set
        )
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.upstream(
                TestingCommons.api(), methods
        ))
        multistream.customHead = Mock(Head)
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(multistreamHolder)
        nativeCall.rpcReaderFactory = Mock(RpcReaderFactory) {
            1 * create(_) >> Mock(RpcReader) {
                1 * read(_) >> Mono.just(new RpcReader.Result("\"0xab\"".bytes, null, 1, ups))
            }
        }
        def call = new NativeCall.ValidCallContext(1, 10, multistream, Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_getFilterChanges", []),
                new NativeCall.WithFilterIdDecorator(), new NativeCall.CreateFilterDecorator(), null, "reqId", 1)

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(1))
        def act = objectMapper.readValue(resp.result, Object)
        then:
        act == "0xabff"
        resp.nonce == 10
    }

    def "Decorate eth_newFilter result with short nodeId"() {
        setup:
        def ups = Mock(Upstream) {
            _ * nodeId() >> (byte)1
        }
        def quorum = new AlwaysQuorum()
        def methods = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET),
                [] as Set, [] as Set, ["filter"] as Set, [] as Set
        )
        def multistream = new MultistreamHolderMock.EthereumMultistreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.upstream(
                TestingCommons.api(), methods
        ))
        multistream.customHead = Mock(Head)
        def multistreamHolder = Mock(MultistreamHolder) {
            _ * it.observeChains() >> Flux.empty()
        }
        def nativeCall = nativeCall(multistreamHolder)
        nativeCall.rpcReaderFactory = Mock(RpcReaderFactory) {
            1 * create(_) >> Mock(RpcReader) {
                1 * read(_) >> Mono.just(new RpcReader.Result("\"0xab\"".bytes, null, 1, ups))
            }
        }
        def call = new NativeCall.ValidCallContext(1, 10, multistream, Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_getFilterChanges", []),
                new NativeCall.WithFilterIdDecorator(), new NativeCall.CreateFilterDecorator(), null, "reqId", 1)

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(1))
        def act = objectMapper.readValue(resp.result, Object)
        then:
        act == "0xab01"
        resp.nonce == 10
    }

    @Ignore
    //TODO
    def "Calls cache before remote"() {
        setup:
        def nativeCall = nativeCall()
        def api = TestingCommons.api()
        def upstream = TestingCommons.multistream(api)

        def ctx = new NativeCall.ValidCallContext<NativeCall.ParsedCallDetails>(10, null,
                upstream,
                Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1)
        when:
        nativeCall.fetch(ctx)
        then:
        1 * cacheMock.execute(10, "eth_test", []) >> Mono.empty()
    }

    @Ignore
    //TODO
    def "Uses cached value"() {
        setup:
        def nativeCall = nativeCall()
        def upstream = TestingCommons.multistream(TestingCommons.api())

        def ctx = new NativeCall.ValidCallContext<NativeCall.ParsedCallDetails>(10, null,
                upstream,
                Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", []), "reqId", 1)
        when:
        def act = nativeCall.fetch(ctx)
        then:
        1 * cacheMock.execute(10, "eth_test", []) >> Mono.just('{"result": "foo"}'.bytes)
        new String(act.block().payload) == '{"result": "foo"}'
    }
}
