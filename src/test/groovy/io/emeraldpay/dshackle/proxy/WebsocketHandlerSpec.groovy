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
package io.emeraldpay.dshackle.proxy

import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.rpc.NativeSubscribe
import io.emeraldpay.etherjar.rpc.json.RequestJson
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import spock.lang.Specification

import java.time.Duration

class WebsocketHandlerSpec extends Specification {

    def requestHandlerFactory = new AccessHandlerHttp.NoOpFactory()
    def requestHandler = new AccessHandlerHttp.NoOpHandler()

    def "Parse standard RPC request"() {
        setup:
        def handler = new WebsocketHandler(
                new ReadRpcJson(), Stub(WriteRpcJson), Stub(NativeCall), Stub(NativeSubscribe), requestHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )
        when:
        def act = handler.parseRequest('{"id": 5, "jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["0x100001", false]}'.bytes, Chain.ETHEREUM)
                .block(Duration.ofSeconds(1))

        then:
        act.id == 5
        act.method == "eth_getBlockByNumber"
        act.params == ["0x100001", false]
    }

    def "Parse to empty an invalid request"() {
        setup:
        Counter errorMetric = Mock(Counter) {
            1 * increment()
        }
        ProxyServer.RequestMetricsFactory metrics = Mock(ProxyServer.RequestMetricsFactory) {
            1 * get(Chain.ETHEREUM, "invalid_method") >> Mock(ProxyServer.RequestMetrics) {
                1 * it.errorMetric >> errorMetric
            }
        }
        def handler = new WebsocketHandler(
                new ReadRpcJson(), Stub(WriteRpcJson), Stub(NativeCall), Stub(NativeSubscribe), requestHandlerFactory, metrics
        )
        when:
        def act = handler.parseRequest('hello world'.bytes, Chain.ETHEREUM)
                .block(Duration.ofSeconds(1))

        then:
        act == null
    }

    def "Parse to empty a batch request"() {
        setup:
        def req1 = '{"id": 5, "jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["0x100001", false]}'
        def handler = new WebsocketHandler(
                new ReadRpcJson(), Stub(WriteRpcJson), Stub(NativeCall), Stub(NativeSubscribe), requestHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )
        when:
        def act = handler.parseRequest("[$req1]".bytes, Chain.ETHEREUM)
                .block(Duration.ofSeconds(1))

        then:
        act == null
    }

    def "Respond to a single call"() {
        setup:
        def response = new NativeCall.CallResult(0, '{"foo": 1}'.bytes, null)

        def nativeCall = Mock(NativeCall) {
            1 * it.nativeCallResult(_) >> Flux.fromIterable([response])
        }
        def handler = new WebsocketHandler(
                new ReadRpcJson(), new WriteRpcJson(), nativeCall, Stub(NativeSubscribe), requestHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )

        def request = new RequestJson("foo_test", [], 2)
        when:
        def act = handler.respond(Chain.ETHEREUM, new HashMap<String, Sinks.One<Boolean>>(), Flux.just(request), requestHandler)
                .single()
                .block(Duration.ofSeconds(1))
        then:
        act == '{"jsonrpc":"2.0","id":2,"result":{"foo": 1}}'
    }

    def "Respond to a subscription call"() {
        setup:
        def response1 = [foo: 1]
        def response2 = [foo: 2]

        def nativeSubscribe = Mock(NativeSubscribe) {
            1 * it.subscribe(Chain.ETHEREUM, "foo_test", null) >> Flux.fromIterable([response1, response2])
        }
        def handler = new WebsocketHandler(
                new ReadRpcJson(), new WriteRpcJson(), Stub(NativeCall), nativeSubscribe, requestHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )

        def request = new RequestJson("eth_subscribe", ["foo_test"], 2)
        when:
        def act = handler.respond(Chain.ETHEREUM, new HashMap<String, Sinks.One<Boolean>>(), Flux.just(request), requestHandler)
                .collectList()
                .block(Duration.ofSeconds(1))
        then:
        act[0] == '{"jsonrpc":"2.0","id":2,"result":"1"}'
        act[1] == '{"jsonrpc":"2.0","method":"eth_subscription","params":{"result":{"foo":1},"subscription":"1"}}'
        act[2] == '{"jsonrpc":"2.0","method":"eth_subscription","params":{"result":{"foo":2},"subscription":"1"}}'
        act.size() == 3
    }

    def "Unsubscribe"() {
        setup:

        def handler = new WebsocketHandler(
                new ReadRpcJson(), new WriteRpcJson(), Stub(NativeCall), Stub(NativeSubscribe), requestHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )

        def control = new HashMap<String, Sinks.One<Boolean>>()
        Sinks.One<Boolean> sink = Sinks.one();
        control["5"] = sink
        def request = new RequestJson("eth_unsubscribe", ["5"], 0)
        when:
        def act = handler.respond(Chain.ETHEREUM, control, Flux.just(request), requestHandler)
                .single()
                .block(Duration.ofSeconds(1))
        def sinkResponse = sink.asMono().block()
        then:
        act == '{"jsonrpc":"2.0","id":0,"result":true}'
        sinkResponse != null
    }

    def "Unsubscribe when no subscription"() {
        setup:

        def handler = new WebsocketHandler(
                new ReadRpcJson(), new WriteRpcJson(), Stub(NativeCall), Stub(NativeSubscribe), requestHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )

        def control = new HashMap<String, Sinks.One<Boolean>>()
        def request = new RequestJson("eth_unsubscribe", ["5"], 0)
        when:
        def act = handler.respond(Chain.ETHEREUM, control, Flux.just(request), requestHandler)
                .single()
                .block(Duration.ofSeconds(1))
        then:
        act == '{"jsonrpc":"2.0","id":0,"result":false}'
    }
}
