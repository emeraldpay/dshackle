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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.rpc.NativeSubscribe
import io.emeraldpay.etherjar.rpc.json.RequestJson
import io.emeraldpay.etherjar.rpc.json.ResponseJson
import io.emeraldpay.grpc.Chain
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import org.apache.commons.lang3.StringUtils
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.http.websocket.WebsocketInbound
import reactor.netty.http.websocket.WebsocketOutbound
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiFunction

/**
 * Responds to Websocket requests made to the Ethereum Proxy Server
 */
class WebsocketHandler(
    private val readRpcJson: ReadRpcJson,
    writeRpcJson: WriteRpcJson,
    nativeCall: NativeCall,
    private val nativeSubscribe: NativeSubscribe,
    requestMetrics: ProxyServer.RequestMetricsFactory,
) : BaseHandler(writeRpcJson, nativeCall, requestMetrics) {

    companion object {
        private val log = LoggerFactory.getLogger(WebsocketHandler::class.java)
    }

    private val subscriptionId = AtomicLong(0)

    fun nextSubscriptionId(): String {
        val n = subscriptionId.incrementAndGet()
        return StringUtils.leftPad(n.toString(16), 16, "0")
    }

    fun proxy(routeConfig: ProxyConfig.Route): BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> {
        return BiFunction { req, resp ->
            val requests: Flux<RequestJson<Any>> = req.aggregateFrames()
                .receiveFrames()
                .map { ByteBufInputStream(it.content()).readAllBytes() }
                .flatMap(this@WebsocketHandler::parseRequest)

            val eventHandler: AccessHandlerHttp.RequestHandler = AccessHandlerHttp.NoOpHandler()
            val responses = respond(routeConfig.blockchain, requests, eventHandler)
                .map { Unpooled.wrappedBuffer(it.toByteArray()) }

            resp.send(responses)
                .then()
        }
    }

    fun parseRequest(data: ByteArray): Mono<RequestJson<Any>> {
        // try to parse JSON call. If received an invalid value just silently ignore it, that's what other Ethereum servers do
        try {
            val type = readRpcJson.getType(data)
            // WS is not supposed to have batches, so ignore them too
            if (type != ProxyCall.RpcType.SINGLE) {
                return Mono.empty()
            }
            val items = readRpcJson.extract(type, data)
            if (items.isEmpty()) {
                // empty should never happen for a SINGLE type of request, but anyway, just return nothing
                return Mono.empty()
            }
            return Mono
                .just(items.first())
                .map(readRpcJson.jsonExtractor)
                .onErrorResume { Mono.empty() }
        } catch (t: Throwable) {
            return Mono.empty()
        }
    }

    fun respond(blockchain: Chain, requests: Flux<RequestJson<Any>>, eventHandler: AccessHandlerHttp.RequestHandler): Flux<String> {
        return requests.flatMap { call ->
            val method = call.method
            if (method == "eth_subscribe") {
                val methodParams = splitMethodParams(call.params)
                if (methodParams != null) {
                    val subscriptionId = nextSubscriptionId()
                    // first need to respond with ID of the subscription, and the following responses would have it in "subscription" param
                    val start = ResponseJson<String, Any>().also {
                        it.id = call.id
                        it.result = subscriptionId
                    }
                    // produce actual responses
                    val responses = nativeSubscribe
                        .subscribe(blockchain, methodParams.first, methodParams.second)
                        .map { event ->
                            WsSubscriptionResponse(params = WsSubscriptionData(event, subscriptionId))
                        }
                    Flux.concat(Mono.just(start), responses)
                        .map { Global.objectMapper.writeValueAsString(it) }
                } else {
                    Mono.empty()
                }
            } else {
                val proxyCall = readRpcJson.convertToNativeCall(ProxyCall.RpcType.SINGLE, listOf(call))
                execute(blockchain, proxyCall, eventHandler)
            }
        }
    }

    fun splitMethodParams(params: List<Any?>): Pair<String, Any?>? {
        if (params.isEmpty()) {
            return null
        }
        if (params.size == 1) {
            return Pair(params.first().toString(), null)
        }
        if (params.size == 2) {
            return Pair(params.first().toString(), params[1])
        }
        return null
    }

    // classes only to render WebSocket subscription response.
    // the difference with standard JSON RPC responses that it
    // (1) it doesn't have id on the top level, but rather as part of params,
    // and (2) it has the `method` field
    data class WsSubscriptionResponse(
        val jsonrpc: String = "2.0",
        val method: String = "eth_subscription",
        val params: WsSubscriptionData,
    )

    data class WsSubscriptionData(
        val result: Any?,
        val subscription: String
    )
}
