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
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessLogHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.rpc.NativeSubscribe
import io.emeraldpay.etherjar.rpc.RequestJson
import io.emeraldpay.etherjar.rpc.ResponseJson
import io.netty.buffer.ByteBufInputStream
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.netty.http.websocket.WebsocketInbound
import reactor.netty.http.websocket.WebsocketOutbound
import java.util.Base64
import java.util.UUID
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
    private val accessHandler: AccessLogHandlerHttp.HandlerFactory,
    private val requestMetrics: ProxyServer.RequestMetricsFactory,
) : BaseHandler(writeRpcJson, nativeCall, requestMetrics) {

    companion object {
        private val log = LoggerFactory.getLogger(WebsocketHandler::class.java)
    }

    private val subscriptionId = AtomicLong(0)

    fun nextSubscriptionId(): String {
        val n = subscriptionId.incrementAndGet()
        return n.toString(16)
    }

    fun proxy(routeConfig: ProxyConfig.Route): BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> {
        return BiFunction { req, resp ->
            // each connection keeps a list of subscription controllers
            val control = HashMap<String, Sinks.One<Boolean>>()

            val requests: Flux<RequestJson<Any>> = req.aggregateFrames()
                .receiveFrames()
                .map { ByteBufInputStream(it.content()).readAllBytes() }
                .flatMap { parseRequest(it, routeConfig.blockchain) }

            val eventHandler = accessHandler.start(req, routeConfig.blockchain)

            val responses = respond(routeConfig.blockchain, control, requests, eventHandler)

            resp.sendString(responses, Charsets.UTF_8)
                .then()
        }
    }

    fun parseRequest(data: ByteArray, blockchain: Chain): Mono<RequestJson<Any>> {
        // try to parse JSON call. If received an invalid value just silently ignore it, that's what other Ethereum servers do
        try {
            val type = readRpcJson.getType(data)
            // WS is not supposed to have batches, so ignore them too
            if (type != ProxyCall.RpcType.SINGLE) {
                requestMetrics.get(blockchain, "batch").errorMetric.increment()
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
                .onErrorResume {
                    log.debug("Failed to process request JSON with: ${it.javaClass} ${it.message}")
                    requestMetrics.get(blockchain, "invalid_method").errorMetric.increment()
                    Mono.empty()
                }
        } catch (t: Throwable) {
            log.warn("Unhandled exception processing request message: ${t.javaClass} ${t.message}")
            requestMetrics.get(blockchain, "invalid_method").errorMetric.increment()
            return Mono.empty()
        }
    }

    fun respond(
        blockchain: Chain,
        control: MutableMap<String, Sinks.One<Boolean>>,
        requests: Flux<RequestJson<Any>>,
        eventHandlerFactory: AccessLogHandlerHttp.WsHandlerFactory
    ): Flux<String> {
        return requests.flatMap { call ->
            val method = call.method
            val requestId = UUID.randomUUID()
            if (method == "eth_subscribe") {
                val methodParams = splitMethodParams(call.params)
                if (methodParams != null) {
                    val eventHandler: AccessLogHandlerHttp.SubscriptionHandler = eventHandlerFactory.subscribe(requestId)
                    val subscriptionId = nextSubscriptionId()
                    eventHandler.onRequest(
                        methodParams.let { mp ->
                            // TODO ineffective to encode the params each time just to get size, ideally should get a reference to the original JSON bytes
                            // but it doesn't happen very ofter, only on initial subscribe only for logs with filter
                            Pair(mp.first, mp.second?.let { Global.objectMapper.writeValueAsBytes(it) })
                        }
                    )
                    val currentControl = Sinks.one<Boolean>()
                    control[subscriptionId] = currentControl
                    // first need to respond with ID of the subscription, and the following responses would have it in "subscription" param
                    val start = ResponseJson<String, Any>().also {
                        it.id = call.id
                        it.result = subscriptionId
                    }
                    // produce actual responses
                    val responses = nativeSubscribe
                        .subscribe(blockchain, methodParams.first, methodParams.second)
                        .map { event ->
                            WsSubscriptionResponse(params = WsSubscriptionData.of(event, subscriptionId))
                        }
                        .takeUntilOther(currentControl.asMono())
                    Flux.concat(Mono.just(start), responses)
                        .map { Global.objectMapper.writeValueAsString(it) }
                        .doOnNext {
                            eventHandler.onResponse(it.length.toLong())
                        }
                } else {
                    requestMetrics.get(blockchain, "eth_subscribe").errorMetric.increment()
                    // TODO should it produce a 404 to the AccessLog?
                    Mono.empty()
                }
            } else if (method == "eth_unsubscribe") {
                val id = call.params?.getOrNull(0) ?: ""

                // put it to the Access Log with fake id=0 (it doesn't matter, except the later reference)
                val eventHandler: AccessLogHandlerHttp.RequestHandler = eventHandlerFactory.call(requestId)
                eventHandler.onRequest(
                    BlockchainOuterClass.NativeCallRequest.newBuilder()
                        .setChainValue(blockchain.id)
                        .addItems(
                            BlockchainOuterClass.NativeCallItem.newBuilder()
                                .setId(0)
                                .setMethod("eth_unsubscribe")
                                .setPayload(ByteString.copyFromUtf8("[\"$id\"]"))
                                .build()
                        )
                        .build()
                )

                val p = control.remove(id.toString())
                val success = p?.tryEmitValue(true)?.isSuccess ?: false
                val response = ResponseJson<Boolean, Any>().also {
                    it.id = call.id
                    it.result = success
                }
                Mono.just(response)
                    .map { Global.objectMapper.writeValueAsString(it) }
                    .doOnNext { eventHandler.onResponse(NativeCall.CallResult.ok(0, null, it.toByteArray(), null)) }
                    .doFinally { eventHandler.close() }
            } else {
                val eventHandler: AccessLogHandlerHttp.RequestHandler = eventHandlerFactory.call(requestId)
                val proxyCall = readRpcJson.convertToNativeCall(ProxyCall.RpcType.SINGLE, listOf(call))
                Mono.from(execute(blockchain, proxyCall, eventHandler))
                    .contextWrite(Global.monitoring.egress.start(requestId))
                    // thought the event handler is used in execute
                    // it still needs to be closed at the end, so it can render the logs
                    .doFinally { eventHandler.close() }
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
    ) {

        companion object {
            fun of(result: Any?, subscription: String) = WsSubscriptionData(
                when (result) {
                    is ByteArray -> Base64.getEncoder().encodeToString(result)
                    else -> result
                },
                subscription
            )
        }
    }
}
