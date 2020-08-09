/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.TlsSetup
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.rpc.RpcException
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.handler.ssl.SslContextBuilder
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.DisposableServer
import reactor.netty.http.server.HttpServer
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import reactor.netty.http.server.HttpServerRoutes
import java.io.File
import java.util.function.BiFunction

/**
 * HTTP Proxy Server
 */
class ProxyServer(
        private var config: ProxyConfig,
        private val readRpcJson: ReadRpcJson,
        private val writeRpcJson: WriteRpcJson,
        private val nativeCall: NativeCall,
        private val tlsSetup: TlsSetup
) {

    companion object {
        private val log = LoggerFactory.getLogger(ProxyServer::class.java)
    }

    fun start() {
        if (!config.enabled) {
            log.debug("Proxy server is not enabled")
            return
        }
        log.info("Listening Proxy on ${config.host}:${config.port}")
        var serverBuilder = HttpServer.create()
                .host(config.host)
                .port(config.port)

        tlsSetup.setupServer("proxy", config.tls, false)?.let { sslContext ->
            serverBuilder = serverBuilder.secure { secure -> secure.sslContext(sslContext) }
        }

        val server: DisposableServer = serverBuilder
                .route(this::setupRoutes)
                .bindNow()
    }

    fun setupRoutes(routes: HttpServerRoutes) {
        config.routes.forEach { routeConfig ->
            routes.post("/" + routeConfig.id, proxy(routeConfig))
        }
    }

    fun execute(chain: Common.ChainRef, call: ProxyCall): Publisher<String> {
        val request = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(chain)
                .addAllItems(call.items)
                .build()
        val jsons = nativeCall
                .nativeCall(Mono.just(request))
                .transform(writeRpcJson.toJsons(call))
        return if (call.type == ProxyCall.RpcType.SINGLE) {
            jsons.next()
        } else {
            jsons.transform(writeRpcJson.asArray())
        }
    }

    fun processRequest(chain: Common.ChainRef, request: Mono<ByteArray>): Flux<ByteBuf> {
        return request.map(readRpcJson)
                .flatMapMany { call -> execute(chain, call) }
                .onErrorResume(RpcException::class.java) { err ->
                    val id = err.details?.let {
                        if (it is JsonRpcResponse.Id) it else JsonRpcResponse.IntId(-1)
                    } ?: JsonRpcResponse.IntId(-1)

                    val json = JsonRpcResponse.error(err.code, err.rpcMessage, id)
                    Mono.just(Global.objectMapper.writeValueAsString(json))
                }
                .map { Unpooled.wrappedBuffer(it.toByteArray()) }
    }

    fun proxy(routeConfig: ProxyConfig.Route): BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
        val chain = Common.ChainRef.forNumber(routeConfig.blockchain.id)
        return BiFunction { req, resp ->
            val request = req.receive()
                    .aggregate()
                    .asByteArray()
            val results = processRequest(chain, request)
            resp.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .send(results)
        }
    }
}