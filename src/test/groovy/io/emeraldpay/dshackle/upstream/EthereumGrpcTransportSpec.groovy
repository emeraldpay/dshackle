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
    def ethereumTargets = new EthereumTargets(objectMapper, Chain.ETHEREUM)

    def "Make simple call"() {
        setup:
        def callData = [:]
        def otherSideUpstreams = Mock(Upstreams)
        def otherSideAggr = Mock(AggregatedUpstreams)
        def otherSideNativeCall = new NativeCall(otherSideUpstreams, objectMapper)
        def otherSideApi = new EthereumApiMock(Mock(RpcClient), objectMapper, Chain.ETHEREUM, otherSideAggr)

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
        1 * otherSideAggr.getApis(_) >> [otherSideApi].iterator()
        _ * otherSideAggr.getHead() >> Stub(EthereumHead)
        _ * otherSideAggr.getTargets() >> ethereumTargets
        status.failed == 0
        status.succeed == 1
        status.total == 1
        callData.request != null
        with((BlockchainOuterClass.NativeCallRequest)callData.request) {
            chain.number == Chain.ETHEREUM.id
            itemsCount == 1
            with(getItems(0)) {
                target == "eth_test"
                payload.toStringUtf8() == "[1]"
            }
        }
        f.get() == "bar"
    }

    def "Make few calls"() {
        setup:
        def callData = [:]
        def otherSideUpstreams = Mock(Upstreams)
        def otherSideAggr = Mock(AggregatedUpstreams)
        def otherSideNativeCall = new NativeCall(otherSideUpstreams, objectMapper)
        def otherSideApi = new EthereumApiMock(Mock(RpcClient), objectMapper, Chain.ETHEREUM, otherSideAggr)

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
        1 * otherSideAggr.getApis(_) >> [otherSideApi].multiply(3).iterator()
        _ * otherSideAggr.getHead() >> Stub(EthereumHead)
        _ * otherSideAggr.getTargets() >> ethereumTargets
        status.failed == 0
        status.succeed == 2
        status.total == 2
        callData.request != null
        with((BlockchainOuterClass.NativeCallRequest)callData.request) {
            chain.number == Chain.ETHEREUM.id
            itemsCount == 2
            with(getItems(0)) {
                target == "eth_test"
                payload.toStringUtf8() == "[1]"
            }
            with(getItems(1)) {
                target == "eth_test2"
                payload.toStringUtf8() == "[2,\"3\"]"
            }
        }
        f1.get() == "bar"
        f2.get() == "baz"
    }

}
