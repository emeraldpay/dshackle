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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.TlsSetup
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.rpc.NativeSubscribe
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory
import reactor.netty.http.server.HttpServer
import reactor.netty.http.server.HttpServerRoutes
import java.util.EnumMap
import java.util.concurrent.ConcurrentHashMap

/**
 * HTTP Proxy Server
 */
class ProxyServer(
    private var config: ProxyConfig,
    readRpcJson: ReadRpcJson,
    writeRpcJson: WriteRpcJson,
    nativeCall: NativeCall,
    nativeSubscribe: NativeSubscribe,
    private val tlsSetup: TlsSetup,
    accessHandler: AccessHandlerHttp.HandlerFactory
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

    private val requestMetrics: RequestMetricsFactory = if (Global.metricsExtended) {
        ExtendedRequestMetrics()
    } else {
        StandardRequestMetrics()
    }

    private val httpHandler = HttpHandler(config, readRpcJson, writeRpcJson, nativeCall, accessHandler, requestMetrics)
    private val wsHandler: WebsocketHandler? = if (config.websocketEnabled) {
        WebsocketHandler(readRpcJson, writeRpcJson, nativeCall, nativeSubscribe, accessHandler, requestMetrics)
    } else null

    fun start() {
        if (!config.enabled) {
            log.debug("Proxy server is not enabled")
            return
        }
        log.info("Start HTTP JSON RPC Proxy on ${connectAddress("http")}")
        if (config.websocketEnabled) {
            log.info("Start Websocket JSON RPC Proxy on ${connectAddress("ws")}")
        }
        var serverBuilder = HttpServer.create()
            .doOnChannelInit { _, channel, _ ->
                channel.pipeline().addFirst(errorHandler)
            }
            .host(config.host)
            .port(config.port)

        tlsSetup.setupServer("proxy", config.tls, false)?.let { sslContext ->
            serverBuilder = serverBuilder.secure { secure -> secure.sslContext(sslContext) }
        }

        serverBuilder
            .route(this::setupRoutes)
            .runOn(NioEventLoopGroup())
            .bindNow()
    }

    fun setupRoutes(routes: HttpServerRoutes) {
        config.routes.forEach { routeConfig ->
            // TODO implement a manual handling of the routes and WS upgrade to have a better control over the connection and improve the access logging
            routes.post("/" + routeConfig.id, httpHandler.proxy(routeConfig))
            routes.options("/" + routeConfig.id, httpHandler.preflight())
            if (config.websocketEnabled && wsHandler != null) {
                routes.ws("/" + routeConfig.id, wsHandler.proxy(routeConfig))
            }
        }
    }

    fun connectAddress(baseSchema: String): String {
        val schema = if (config.tls != null) baseSchema + "s" else baseSchema
        return "$schema://${config.host}:${config.port}"
    }

    interface RequestMetricsFactory {
        fun get(chain: Chain, method: String): RequestMetrics
    }

    /**
     * Standard metrics
     */
    class StandardRequestMetrics : RequestMetricsFactory {
        private val current = EnumMap<Chain, RequestMetrics>(Chain::class.java)

        override fun get(chain: Chain, method: String): RequestMetrics {
            return current.getOrPut(chain) { RequestMetricsBasic(chain) }
        }
    }

    class ExtendedRequestMetrics : RequestMetricsFactory {
        private val current = ConcurrentHashMap<Ref, RequestMetrics>()

        override fun get(chain: Chain, method: String): RequestMetrics =
            current.computeIfAbsent(Ref(chain, method)) {
                RequestMetricsWithMethod(it.chain, it.method)
            }

        data class Ref(val chain: Chain, val method: String)
    }

    interface RequestMetrics {
        val callMetric: Timer
        val errorMetric: Counter
        val failMetric: Counter
        val requestMetric: Counter
    }

    class RequestMetricsBasic(chain: Chain) : RequestMetrics {
        override val callMetric = Timer.builder("request.jsonrpc.call")
            .description("Time to process a call")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        override val errorMetric = Counter.builder("request.jsonrpc.err")
            .description("Number of requests ended with an error response")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        override val failMetric = Counter.builder("request.jsonrpc.fail")
            .description("Number of requests failed to process")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        override val requestMetric = Counter.builder("request.jsonrpc.request.total")
            .description("Number of requests")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
    }

    class RequestMetricsWithMethod(chain: Chain, method: String) : RequestMetrics {
        override val callMetric = Timer.builder("request.jsonrpc.call")
            .description("Time to process a call")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
        override val errorMetric = Counter.builder("request.jsonrpc.err")
            .description("Number of requests ended with an error response")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
        override val failMetric = Counter.builder("request.jsonrpc.fail")
            .description("Number of requests failed to process")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
        override val requestMetric = Counter.builder("request.jsonrpc.request.total")
            .description("Number of requests")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
    }
}
