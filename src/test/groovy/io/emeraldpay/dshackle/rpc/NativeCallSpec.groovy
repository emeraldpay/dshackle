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
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.NonEmptyQuorum
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.ethereum.NativeCallRouter
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Ignore
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.TimeoutException

class NativeCallSpec extends Specification {

    def objectMapper = TestingCommons.objectMapper()

    def "Tries router first"() {
        def routedApi = Mock(Reader) {
            1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.just(new JsonRpcResponse("1".bytes, null))
        }
        def upstream = Mock(Multistream) {
            1 * getRoutedApi(_) >> Mono.just(routedApi)
        }
        def upstreams = Stub(MultistreamHolder)

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(
                1, upstream, Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", [])
        )

        when:
        def act = nativeCall.fetch(ctx).block(Duration.ofSeconds(1))
        then:
        act.payload == "1".bytes
    }

    def "Return error if router denied the requests"() {
        def routedApi = Mock(Reader) {
            1 * read(new JsonRpcRequest("eth_test", [])) >> Mono.error(new RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Test message"))
        }
        def upstream = Mock(Multistream) {
            1 * getRoutedApi(_) >> Mono.just(routedApi)
        }
        def upstreams = Stub(MultistreamHolder)

        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def ctx = new NativeCall.CallContext<NativeCall.ParsedCallDetails>(
                15, upstream, Selector.empty, new AlwaysQuorum(),
                new NativeCall.ParsedCallDetails("eth_test", [])
        )

        when:
        def act = nativeCall.fetch(ctx) //.block(Duration.ofSeconds(1))
        then:
        StepVerifier.create(act)
                .expectErrorMatches { t ->
                    t instanceof NativeCall.CallFailure &&
                            t.id == 15 &&
                            t.reason instanceof RpcException &&
                            t.reason.rpcMessage == "Test message"
                }
                .verify(Duration.ofSeconds(1))
    }

    def "Quorum is applied"() {
        setup:
        def quorum = new AlwaysQuorum()

        def nativeCall = new NativeCall(Stub(MultistreamHolder), TestingCommons.objectMapper())
        nativeCall.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(_) >> Mono.just(new QuorumRpcReader.Result("\"foo\"".bytes, 1))
            }
        }
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        when:
        def resp = nativeCall.executeOnRemote(call).block(Duration.ofSeconds(1))
        def act = objectMapper.readValue(resp.payload, Object)
        then:
        act == "foo"
    }

    def "Returns error if no quorum"() {
        setup:
        def quorum = new AlwaysQuorum()

        def nativeCall = new NativeCall(Stub(MultistreamHolder), TestingCommons.objectMapper())
        nativeCall.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(_) >> Mono.empty()
            }
        }
        def call = new NativeCall.CallContext(1, TestingCommons.aggregatedUpstream(TestingCommons.api()), Selector.empty, quorum,
                new NativeCall.ParsedCallDetails("eth_test", []))

        when:
        def resp = nativeCall.executeOnRemote(call)
        then:
        StepVerifier.create(resp)
                .expectErrorMatches({ t -> t instanceof NativeCall.CallFailure && t.id == 1 })
                .verify(Duration.ofSeconds(1))
    }

    def "Packs call exception into response with id"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
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
        def upstreams = Stub(MultistreamHolder)
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
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def json = [jsonrpc:"2.0", id:1, result: "foo"]

        when:
        def resp = nativeCall.buildResponse(
                new NativeCall.CallContext<byte[]>(1561, TestingCommons.aggregatedUpstream(TestingCommons.api()), Selector.empty, new AlwaysQuorum(), objectMapper.writeValueAsBytes(json))
        )
        then:
        resp.id == 1561
        resp.succeed
        objectMapper.readValue(resp.payload.toByteArray(), Map.class) == [jsonrpc:"2.0", id:1, result: "foo"]
    }

    def "Returns error for invalid chain"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
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
        def upstreams = Mock(MultistreamHolder)
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

    @Ignore
    //TODO
    def "Calls cache before remote"() {
        setup:
        def upstreams = Stub(MultistreamHolder)
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def api = TestingCommons.api()
        def upstream = TestingCommons.aggregatedUpstream(api)

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
        def nativeCall = new NativeCall(upstreams, TestingCommons.objectMapper())
        def upstream = TestingCommons.aggregatedUpstream(TestingCommons.api())

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
