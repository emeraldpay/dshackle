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

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.dshackle.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.step.StepCounter
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.util.function.Function

class HttpHandlerSpec extends Specification {

    def "Calls access log handler"() {
        setup:
        def reqItem = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(1)
                .setMethod("test_test")
                .setPayload(ByteString.copyFromUtf8("[]"))
                .build()
        def respItem = new NativeCall.CallResult(1, null, "100".bytes, null, null)
        def req = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                .addItems(reqItem)
                .build()

        NativeCall nativeCall = Mock(NativeCall) {
            1 * nativeCallResult(_) >> Flux.fromIterable([respItem])
        }
        def accessHandler = Mock(AccessHandlerHttp.RequestHandler)
        def accessHandlerFactory = Mock(AccessHandlerHttp.HandlerFactory) {
            _ * it.create(_,) >> accessHandler
        }
        def handler = new HttpHandler(
                new ProxyConfig(),
                new ReadRpcJson(), new WriteRpcJson(),
                nativeCall, accessHandlerFactory, Stub(ProxyServer.RequestMetricsFactory)
        )

        when:
        handler.execute(Chain.ETHEREUM, [reqItem], accessHandler)
                .blockLast()

        then:
        1 * accessHandler.onRequest(req)
        1 * accessHandler.onResponse(respItem)
    }

    def "Return error on invalid request"() {
        setup:
        ReadRpcJson read = Mock(ReadRpcJson) {
            1 * apply(_) >> { throw new RpcException(-32123, "test", new JsonRpcResponse.NumberId(4)) }
        }
        Counter errorMetric = Mock(Counter) {
            1 * increment()
        }
        ProxyServer.RequestMetricsFactory metrics = Mock(ProxyServer.RequestMetricsFactory) {
            1 * get(Chain.ETHEREUM, "invalid_method") >> Mock(ProxyServer.RequestMetrics) {
                1 * it.errorMetric >> errorMetric
            }
        }

        def handler = new HttpHandler(
                new ProxyConfig(),
                read, new WriteRpcJson(),
                Stub(NativeCall), Stub(AccessHandlerHttp.HandlerFactory),
                metrics
        )
        when:

        def act = handler.processRequest(Chain.ETHEREUM, Mono.just("".bytes), new AccessHandlerHttp.NoOpHandler())
                .map { new String(it.array()) }
        then:
        StepVerifier.create(act)
                .expectNext('{"jsonrpc":"2.0","id":4,"error":{"code":-32123,"message":"test"}}')
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Uses NativeCall"() {
        setup:
        NativeCall nativeCall = Mock(NativeCall)
        def predefined = { a -> Flux.just("hello") } as Function

        WriteRpcJson writeRpcJson = Mock {
            1 * toJsons(_) >> predefined
        }


        def handler = new HttpHandler(
                new ProxyConfig(),
                new ReadRpcJson(), writeRpcJson,
                nativeCall, Stub(AccessHandlerHttp.HandlerFactory), Stub(ProxyServer.RequestMetricsFactory)
        )

        def call = new ProxyCall(ProxyCall.RpcType.SINGLE)
        call.ids[1] = 1
        call.items.add(
                BlockchainOuterClass.NativeCallItem.newBuilder()
                        .setMethod("eth_hello")
                        .build()
        )
        when:
        def act = handler.execute(Chain.ETHEREUM, call, new AccessHandlerHttp.NoOpHandler(), false)

        then:
        1 * nativeCall.nativeCallResult(_) >> Flux.just(new NativeCall.CallResult(1, null, "".bytes, null, null))
        StepVerifier.create(act)
                .expectNext("hello")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
