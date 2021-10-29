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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class BaseHandlerSpec extends Specification {

    def requestHandler = new AccessHandlerHttp.NoOpHandler()

    def "Return empty for empty single call"() {
        setup:
        def handler = new BaseHandlerImpl(new WriteRpcJson(), Stub(NativeCall), Stub(ProxyServer.RequestMetricsFactory))
        when:
        def act = Mono.from(handler.execute(Chain.ETHEREUM, new ProxyCall(ProxyCall.RpcType.SINGLE), requestHandler))
                .block(Duration.ofSeconds(1))
        then:
        act == ""
    }

    def "Return empty array for empty batch call"() {
        setup:
        def handler = new BaseHandlerImpl(new WriteRpcJson(), Stub(NativeCall), Stub(ProxyServer.RequestMetricsFactory))
        when:
        def act = Mono.from(handler.execute(Chain.ETHEREUM, new ProxyCall(ProxyCall.RpcType.BATCH), requestHandler))
                .block(Duration.ofSeconds(1))
        then:
        act == "[]"
    }

    def "Execute single call"() {
        setup:
        def nativeCall = Mock(NativeCall)
        def handler = new BaseHandlerImpl(new WriteRpcJson(), nativeCall, Stub(ProxyServer.RequestMetricsFactory))

        def request = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setMethod("eth_test")
                .setId(0)
                .build()
        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.items.add(request)
        call.ids[0] = 5
        def response = new NativeCall.CallResult(0, '{"foo": 1}'.bytes, null)
        when:
        def act = Flux.from(handler.execute(Chain.ETHEREUM, call, requestHandler))
                .collectList()
                .block(Duration.ofSeconds(1))
                .join("")
        then:
        act == '{"jsonrpc":"2.0","id":5,"result":{"foo": 1}}'
        1 * nativeCall.nativeCallResult(_) >> Flux.fromIterable([response])
    }

    def "Execute batch call with one item"() {
        setup:
        def nativeCall = Mock(NativeCall)
        def handler = new BaseHandlerImpl(new WriteRpcJson(), nativeCall, Stub(ProxyServer.RequestMetricsFactory))

        def request = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setMethod("eth_test")
                .setId(0)
                .build()
        def call = new ProxyCall(ProxyCall.RpcType.BATCH)
        call.items.add(request)
        call.ids[0] = 5
        def response = new NativeCall.CallResult(0, '{"foo": 1}'.bytes, null)
        when:
        def act = Flux.from(handler.execute(Chain.ETHEREUM, call, requestHandler))
                .collectList()
                .block(Duration.ofSeconds(1))
                .join("")
        then:
        act == '[{"jsonrpc":"2.0","id":5,"result":{"foo": 1}}]'
        1 * nativeCall.nativeCallResult(_) >> Flux.fromIterable([response])
    }

    class BaseHandlerImpl extends BaseHandler {

        BaseHandlerImpl(@NotNull WriteRpcJson writeRpcJson, @NotNull NativeCall nativeCall, @NotNull ProxyServer.RequestMetricsFactory requestMetrics) {
            super(writeRpcJson, nativeCall, requestMetrics)
        }
    }
}
