package io.emeraldpay.dshackle.rpc

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.test.EthereumApiMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.AlwaysQuorum
import io.emeraldpay.dshackle.upstream.CachingEthereumApi
import io.emeraldpay.dshackle.upstream.CallQuorum
import io.emeraldpay.dshackle.upstream.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.EthereumApi
import io.emeraldpay.dshackle.upstream.NonEmptyQuorum
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.RpcClient
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import reactor.util.function.Tuples
import spock.lang.Specification

import java.time.Duration

class NativeCallSpec extends Specification {

    def objectMapper = TestingCommons.objectMapper()

    def "Quorum is applied"() {
        setup:
        def quorum = Spy(new AlwaysQuorum())
        def upstreams = Stub(Upstreams)
        RpcClient rpcClient = Stub(RpcClient)
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
        (2..3) * quorum.isResolved() // 2 times during api call (before and after) + 1 time in a filter after
        1 * quorum.record(_, _)
        1 * quorum.getResult()
    }

    def "Quorum may return not first received value"() {
        setup:
        def quorum = Spy(new NonEmptyQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        RpcClient rpcClient = Stub(RpcClient)
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
        (3..4) * quorum.isResolved()
        2 * quorum.record(_, _)
        1 * quorum.getResult()
    }

    def "Returns error if no quorum"() {
        setup:
        def quorum = Spy(new NonEmptyQuorum(TestingCommons.rpcConverter(), 3))

        def upstreams = Stub(Upstreams)
        RpcClient rpcClient = Stub(RpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        apiMock.upstream = Stub(Upstream)

        apiMock.answer("eth_test", [], null, 3)
        apiMock.answerOnce("eth_test", [], "foo")

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(apiMock), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        (4..5) * quorum.isResolved()
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
        1 * upstreams.getUpstream(Chain.TESTNET_MORDEN) >> null
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
}
