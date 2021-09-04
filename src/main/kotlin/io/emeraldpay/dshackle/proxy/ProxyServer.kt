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
import io.emeraldpay.dshackle.ChainValue
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.TlsSetup
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.rpc.RpcException
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
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
import reactor.kotlin.adapter.rxjava.toFlowable
import reactor.kotlin.adapter.rxjava.toSingle
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction

/**
 * HTTP Proxy Server
 */
class ProxyServer(
        private var config: ProxyConfig,
        private val readRpcJson: ReadRpcJson,
        private val writeRpcJson: WriteRpcJson,
        private val nativeCall: NativeCall,
        private val tlsSetup: TlsSetup,
        private val accessHandler: AccessHandlerHttp.HandlerFactory
) {

    companion object {
        private val log = LoggerFactory.getLogger(ProxyServer::class.java)
    }

    private val errorHandler: ChannelHandler = object : ChannelHandler {
        override fun handlerAdded(p0: ChannelHandlerContext?) {
        }

        override fun handlerRemoved(p0: ChannelHandlerContext?) {
        }

        override fun exceptionCaught(p0: ChannelHandlerContext?, p1: Throwable?) {
            // HAProxy makes RST,ACK for health probe, which leads to error like:
            // > 2020-22-09 23:46:34.077 | WARN  |          FluxReceive | [id: 0x228b9b97, L:/172.19.0.3:8545 - R:/172.19.0.5:34856] An exception has been observed post termination, use DEBUG level to see the full stack: io.netty.channel.unix.Errors$NativeIoException: syscall:read(..) failed: Connection reset by peer
            // The error is just upsetting and nothing you can do about it, so ignore it.
            // TODO the implementation makes text lookup, there must be a more optimal way to recognize that error
            p1?.let { t ->
                val reset = t.message?.contains("Connection reset by peer") ?: false
                if (!reset) {
                    log.warn("Connection error. ${p1.javaClass}: ${p1.message}")
                }
            }
        }
    }

    fun start() {
        if (!config.enabled) {
            log.debug("Proxy server is not enabled")
            return
        }
        log.info("Listening Proxy on ${config.host}:${config.port}")
        var serverBuilder = HttpServer.create()
                .doOnChannelInit { _, channel, _ ->
                    channel.pipeline().addFirst(errorHandler)
                }
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

    fun execute(chain: Common.ChainRef, call: ProxyCall, handler: AccessHandlerHttp.RequestHandler): Publisher<String> {
        val request = BlockchainOuterClass.NativeCallRequest.newBuilder()
                .setChain(chain)
                .addAllItems(call.items)
                .build()
        handler.onRequest(request)
        val jsons = nativeCall
                .nativeCallResult(Mono.just(request))
                .doOnNext { handler.onResponse(it) }
                .transform(writeRpcJson.toJsons(call))
        return if (call.type == ProxyCall.RpcType.SINGLE) {
            jsons.next()
        } else {
            jsons.transform(writeRpcJson.asArray())
        }
    }

    fun getEthMethod(call: ProxyCall): String {
        return call.items.get(0).method
    }

    fun processRequest(chain: Chain, request: Mono<ByteArray>, handler: AccessHandlerHttp.RequestHandler): Flux<ByteBuf> {
        val startTime = System.currentTimeMillis()
        val metrics = RequestMetrics(chain, "t")
        metrics.requestMetric.increment()
        return request
                .map(readRpcJson)
                .flatMapMany { call -> 
                    execute(Common.ChainRef.forNumber(chain.id), call, handler) 
                }
                .doOnNext {
                    metrics.callMetric.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
                }
                .onErrorResume(RpcException::class.java) { err ->
                    metrics.errorMetric.increment()
                    val id = err.details?.let {
                        if (it is JsonRpcResponse.Id) it else JsonRpcResponse.NumberId(-1)
                    } ?: JsonRpcResponse.NumberId(-1)

                    val json = JsonRpcResponse.error(err.code, err.rpcMessage, id)
                    Mono.just(Global.objectMapper.writeValueAsString(json))
                }
                .map { Unpooled.wrappedBuffer(it.toByteArray()) }
    }

    fun proxy(routeConfig: ProxyConfig.Route): BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
        //val chain = Common.ChainRef.forNumber(routeConfig.blockchain.id)
        return BiFunction { req, resp ->
            // handle access events
            print(resp.toString())
            val eventHandler = accessHandler.create(req, routeConfig.blockchain)
            val request = req.receive()
                    .aggregate()
                    .asByteArray()
            val results = processRequest(routeConfig.blockchain, request, eventHandler)
                    // make sure that the access log handler is closed at the end, so it can render the logs
                    .doFinally { eventHandler.close() }
            resp.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .send(results)
        }
    }

    class RequestMetrics(chain: Chain, method: String) {
        val callMetric = Timer.builder("request.jsonrpc.call")
                .tags("chain", chain.chainCode, "eth_method", method)
                .register(Metrics.globalRegistry)
        val errorMetric = Counter.builder("request.jsonrpc.err")
                .tags("chain", chain.chainCode, "eth_method", method)
                .register(Metrics.globalRegistry)
        val requestMetric = Counter.builder("request.jsonrpc.request.total")
                .tags("chain", chain.chainCode, "eth_method", method)
                .register(Metrics.globalRegistry)
    }
}
