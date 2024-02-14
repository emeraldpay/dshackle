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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import java.util.function.BiFunction

/**
 * Responds to HTTP requests made to the Ethereum Proxy Server
 */
class HttpHandler(
    private val config: ProxyConfig,
    private val readRpcJson: ReadRpcJson,
    writeRpcJson: WriteRpcJson,
    nativeCall: NativeCall,
    private val accessHandler: AccessHandlerHttp.HandlerFactory,
    private val requestMetrics: ProxyServer.RequestMetricsFactory,
) : BaseHandler(writeRpcJson, nativeCall, requestMetrics) {

    companion object {
        private val log = LoggerFactory.getLogger(HttpHandler::class.java)
    }

    private fun addCorsHeadersIfSet(resp: HttpServerResponse): HttpServerResponse {
        return config.corsOrigin?.let {
            resp.addHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, it)
                .addHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, config.corsAllowedHeaders)
        } ?: resp
    }

    fun preflight(): BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
        return BiFunction { _, resp ->
            addCorsHeadersIfSet(resp).send()
        }
    }

    fun proxy(routeConfig: ProxyConfig.Route): BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
        return BiFunction { req, resp ->
            // handle access events
            val eventHandler = accessHandler.create(req, routeConfig.blockchain)
            val request = req.receive()
                .aggregate()
                .asByteArray()
            val results = processRequest(routeConfig.blockchain, request, eventHandler)
                // make sure that the access log handler is closed at the end, so it can render the logs
                .doFinally { eventHandler.close() }
            addCorsHeadersIfSet(resp)
                .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .send(results)
        }
    }

    fun processRequest(
        chain: Chain,
        request: Mono<ByteArray>,
        handler: AccessHandlerHttp.RequestHandler,
    ): Flux<ByteBuf> {
        return request
            .map(readRpcJson)
            .doOnError {
                log.debug("Failed to process request JSON with: ${it.javaClass} ${it.message}")
                // most of the metrics are handled inside the execute method, including most of the errors.
                // but if we have an invalid JSON it stops here, and we cannot handle it in the main onErrorResume
                // because it's going to be a duplicate call for other errors
                requestMetrics.get(chain, "invalid_method").errorMetric.increment()
            }
            .flatMapMany { call ->
                execute(chain, call, handler, config.preserveBatchOrder)
            }
            .onErrorResume(RpcException::class.java) { err ->
                val id = err.details?.let {
                    if (it is JsonRpcResponse.Id) it else JsonRpcResponse.NumberId(-1)
                } ?: JsonRpcResponse.NumberId(-1)

                val json = JsonRpcResponse.error(err.code, err.rpcMessage, id)
                Mono.just(Global.objectMapper.writeValueAsString(json))
            }
            .map { Unpooled.wrappedBuffer(it.toByteArray()) }
    }
}
