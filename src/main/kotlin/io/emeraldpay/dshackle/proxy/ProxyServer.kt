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
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.grpc.Chain
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
import reactor.netty.http.server.HttpServer
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import reactor.netty.http.server.HttpServerRoutes
import java.util.EnumMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.BiFunction
import kotlin.concurrent.read
import kotlin.concurrent.write

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

    private val requestMetrics: RequestMetricsFactory = if (Global.metricsExtended) {
        ExtendedRequestMetrics()
    } else {
        StandardRequestMetrics()
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

        serverBuilder
            .route(this::setupRoutes)
            .bindNow()
    }

    fun setupRoutes(routes: HttpServerRoutes) {
        config.routes.forEach { routeConfig ->
            routes.post("/" + routeConfig.id, proxy(routeConfig))
        }
    }

    fun execute(chain: Chain, call: ProxyCall, handler: AccessHandlerHttp.RequestHandler): Publisher<String> {
        // return empty response for empty request
        if (call.items.isEmpty()) {
            return if (call.type == ProxyCall.RpcType.BATCH) {
                Mono.just("[]")
            } else {
                Mono.just("")
            }
        }
        val startTime = System.currentTimeMillis()
        // during the execution we know only ID of the call, we use it to find the origin call and associated metrics
        val metricById = { id: Int ->
            call.items.find { it.id == id }?.let { item ->
                requestMetrics.get(chain, item.method)
            }
        }
        val request = BlockchainOuterClass.NativeCallRequest.newBuilder()
            .setChain(Common.ChainRef.forNumber(chain.id))
            .addAllItems(call.items)
            .build()
        handler.onRequest(request)
        val jsons = nativeCall
            .nativeCallResult(Mono.just(request))
            .doOnNext {
                metricById(it.id)?.requestMetric?.increment()
            }
            .doOnNext {
                handler.onResponse(it)
                metricById(it.id)?.callMetric?.record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
            }
            .doOnError {
                // when error happened the whole flux is stopped and no result is produced, so we should mark all the requests as failed
                call.items.forEach { item ->
                    requestMetrics.get(chain, item.method).errorMetric.increment()
                }
            }
            .transform(writeRpcJson.toJsons(call))
        return if (call.type == ProxyCall.RpcType.SINGLE) {
            jsons.next()
        } else {
            jsons.transform(writeRpcJson.asArray())
        }
    }

    fun processRequest(
        chain: Chain,
        request: Mono<ByteArray>,
        handler: AccessHandlerHttp.RequestHandler
    ): Flux<ByteBuf> {
        return request
            .map(readRpcJson)
            .flatMapMany { call ->
                execute(chain, call, handler)
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
            resp.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .send(results)
        }
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

    /**
     * Monitoring that has separate metrics per RPC method.
     * Slightly slower to use than StandardRequestMetrics
     */
    class ExtendedRequestMetrics : RequestMetricsFactory {
        private val current = HashMap<Ref, RequestMetrics>()
        private val lock = ReentrantReadWriteLock()

        override fun get(chain: Chain, method: String): RequestMetrics {
            val ref = Ref(chain, method)
            lock.read {
                val existing = current[ref]
                if (existing != null) {
                    return existing
                }
            }
            lock.write {
                val existing = current[ref]
                if (existing != null) {
                    return existing
                }
                val created = RequestMetricsWithMethod(chain, method)
                current[ref] = created
                return created
            }
        }

        data class Ref(val chain: Chain, val method: String)
    }

    interface RequestMetrics {
        val callMetric: Timer
        val errorMetric: Counter
        val requestMetric: Counter
    }

    class RequestMetricsBasic(chain: Chain) : RequestMetrics {
        override val callMetric = Timer.builder("request.jsonrpc.call")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        override val errorMetric = Counter.builder("request.jsonrpc.err")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
        override val requestMetric = Counter.builder("request.jsonrpc.request.total")
            .tags("chain", chain.chainCode)
            .register(Metrics.globalRegistry)
    }

    class RequestMetricsWithMethod(chain: Chain, method: String) : RequestMetrics {
        override val callMetric = Timer.builder("request.jsonrpc.call")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
        override val errorMetric = Counter.builder("request.jsonrpc.err")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
        override val requestMetric = Counter.builder("request.jsonrpc.request.total")
            .tags("chain", chain.chainCode, "method", method)
            .register(Metrics.globalRegistry)
    }
}
