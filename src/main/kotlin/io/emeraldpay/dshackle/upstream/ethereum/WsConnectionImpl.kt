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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsMessage
import io.emeraldpay.dshackle.upstream.rpcclient.ResponseWSParser
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelOption
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollChannelOption
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.resolver.DefaultAddressResolverGroup
import org.apache.commons.lang3.time.StopWatch
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.BackOffExecution
import org.springframework.util.backoff.ExponentialBackOff
import org.springframework.util.backoff.FixedBackOff
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.netty.http.client.HttpClient
import reactor.netty.http.client.WebsocketClientSpec
import reactor.netty.http.websocket.WebsocketInbound
import reactor.netty.http.websocket.WebsocketOutbound
import reactor.netty.resources.ConnectionProvider
import reactor.util.function.Tuples
import java.net.URI
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

open class WsConnectionImpl(
    private val uri: URI,
    private val origin: URI,
    private val basicAuth: AuthConfig.ClientBasicAuth?,
    private val rpcMetrics: RpcMetrics?,
) : AutoCloseable,
    WsConnection {
    companion object {
        private val log = LoggerFactory.getLogger(WsConnectionImpl::class.java)

        private const val IDS_START = 100

        // WebSocket Frame limit.
        // Default is 65_536, but Geth responds with larger frames,
        // and connection gets dropped with:
        // > io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException: Max frame length of 65536 has been exceeded
        // It's unclear what is the right limit here, but 5mb seems to be working (1mb isn't always working)
        private const val DEFAULT_FRAME_SIZE = 5 * 1024 * 1024

        // The max size from multiple frames that may represent a single message
        // Accept up to 15Mb messages, because Geth is using 15mb, though it's not clear what should be a right value
        private const val DEFAULT_MSG_SIZE = 15 * 1024 * 1024
    }

    var frameSize: Int = DEFAULT_FRAME_SIZE
    var msgSizeLimit: Int = DEFAULT_MSG_SIZE

    // Some upstreams fail to connect if compression is enabled
    var compress: Boolean = false

    private var reconnectBackoff: BackOff =
        ExponentialBackOff().also {
            it.initialInterval = Duration.ofMillis(100).toMillis()
            it.maxInterval = Duration.ofMinutes(5).toMillis()
        }
    private var currentBackOff = reconnectBackoff.start()

    private val parser = ResponseWSParser()

    private val subscriptionResponses =
        Sinks
            .many()
            .multicast()
            .directBestEffort<JsonRpcWsMessage>()

    private var rpcSend =
        Sinks
            .many()
            .unicast()
            .onBackpressureBuffer<JsonRpcRequest>()

    private val disconnects =
        Sinks
            .many()
            .multicast()
            .directBestEffort<Instant>()

    // keeps current in-flight request so a received response can propagate it to the original sender (as Sinks.One)
    private val currentRequests = ConcurrentHashMap<Int, Sinks.One<JsonRpcResponse>>()

    private val sendIdSeq = AtomicInteger(IDS_START)
    private val sendExecutor = Executors.newSingleThreadExecutor()
    private var keepConnection = true
    private var connection: Disposable? = null
    private val reconnecting = AtomicBoolean(false)
    private var onConnectionChange: Consumer<WsConnection.ConnectionStatus> = defaultOnConnectionChange()

    /**
     * true when the connection is actively receiving messages
     */
    private var active: Boolean = false

    override val isConnected: Boolean
        get() = connection != null && !reconnecting.get() && active

    private fun defaultOnConnectionChange(): Consumer<WsConnection.ConnectionStatus> =
        rpcMetrics?.let {
            ConnectionTimeMonitoring(it)
        } ?: Consumer { }

    override fun onConnectionChange(handler: Consumer<WsConnection.ConnectionStatus>?) {
        this.onConnectionChange =
            if (handler != null) {
                handler.andThen(defaultOnConnectionChange())
            } else {
                defaultOnConnectionChange()
            }
    }

    fun setReconnectIntervalSeconds(value: Long) {
        reconnectBackoff = FixedBackOff(value * 1000, FixedBackOff.UNLIMITED_ATTEMPTS)
        currentBackOff = reconnectBackoff.start()
    }

    override fun connect() {
        keepConnection = true
        connectInternal()
    }

    private fun tryReconnectLater() {
        if (!keepConnection) {
            return
        }
        val alreadyReconnecting = reconnecting.getAndSet(true)
        if (alreadyReconnecting) {
            return
        }
        // rpcSend is already CANCELLED, since the subscription owned by the previous connection is gone
        // so we need to create a new Sink. Emit Complete is probably useless, and just in case
        rpcSend.tryEmitComplete()
        rpcSend =
            Sinks
                .many()
                .unicast()
                .onBackpressureBuffer<JsonRpcRequest>()
        val retryInterval = currentBackOff.nextBackOff()
        if (retryInterval == BackOffExecution.STOP) {
            log.warn("Reconnect backoff exhausted. Permanently closing the connection")
            return
        }
        log.info("Reconnect to $uri in ${retryInterval}ms...")
        Global.control.schedule(
            {
                reconnecting.set(false)
                connectInternal()
            },
            retryInterval,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun connectInternal() {
        log.info("Connecting to WebSocket: $uri")
        require(uri.isAbsolute) {
            "Invalid WebSocket URI: $uri"
        }
        connection?.dispose()
        connection =
            HttpClient
                // It maybe not necessary but it seems it sometimes tries to reuse the same connection which was broken before
                .create(ConnectionProvider.newConnection())
                // NODELAY and QUICKACK flags make it working a big faster in a load intensive environment (like ~10% for a 99-perc, though it's hard to measure it correctly)
                // and, in general, as a load balancer it supposed to send/receive data as fast as it possible, so it makes sense to have them by default
                .option(ChannelOption.TCP_NODELAY, true)
                .let {
                    if (Epoll.isAvailable()) {
                        it.option(EpollChannelOption.TCP_QUICKACK, true)
                    } else {
                        it
                    }
                }.resolver(DefaultAddressResolverGroup.INSTANCE)
                .doOnDisconnected {
                    active = false
                    disconnects.tryEmitNext(Instant.now())
                    this.onConnectionChange.accept(WsConnection.ConnectionStatus.DISCONNECTED)
                    log.info("Disconnected from $uri")
                    if (keepConnection) {
                        tryReconnectLater()
                    }
                }.let {
                    if (rpcMetrics != null) {
                        it.doOnChannelInit(rpcMetrics.onChannelInit)
                    } else {
                        it
                    }
                }.doOnError(
                    { _, t ->
                        log.warn("Failed to connect to $uri. Error: ${t.message}")
                        // going to try to reconnect later
                        tryReconnectLater()
                    },
                    { _, t ->
                        log.warn("Failed to process response from $uri. Error: ${t.message}")
                    },
                ).headers { headers ->
                    headers.add(HttpHeaderNames.ORIGIN, origin)
                    basicAuth?.let { auth ->
                        val tmp: String = auth.username + ":" + auth.password
                        val base64password = Base64.getEncoder().encodeToString(tmp.toByteArray())
                        headers.add(HttpHeaderNames.AUTHORIZATION, "Basic $base64password")
                    }
                }.let {
                    if (uri.scheme == "wss") it.secure() else it
                }.websocket(
                    WebsocketClientSpec
                        .builder()
                        .handlePing(true)
                        .compress(compress)
                        .maxFramePayloadLength(frameSize)
                        .build(),
                ).uri(uri)
                .handle { inbound, outbound ->
                    this.onConnectionChange.accept(WsConnection.ConnectionStatus.CONNECTED)
                    // mark as active once connected, because the actual message wouldn't come until a request is sent
                    active = true
                    handle(inbound, outbound)
                }.onErrorResume { t ->
                    log.debug("Dropping WS connection to {}. Error: {}", uri, t.message)
                    Mono.empty<Void>()
                }.subscribe()
    }

    fun handle(
        inbound: WebsocketInbound,
        outbound: WebsocketOutbound,
    ): Publisher<Void> {
        var read = false
        val consumer =
            inbound
                .aggregateFrames(msgSizeLimit)
                .receiveFrames()
                .map { ByteBufInputStream(it.content()).readAllBytes() }
                .filter { it.isNotEmpty() }
                .flatMap {
                    // just make sure it's in active state
                    active = true
                    try {
                        rpcMetrics?.responseSize?.record(it.size.toDouble())
                        val msg = parser.parse(it)
                        if (!read) {
                            if (msg.error != null) {
                                log.warn("Received error ${msg.error.code} from $uri: ${msg.error.message}")
                            } else {
                                // restart backoff only after a successful read from the connection,
                                // otherwise it may restart it even if the connection is faulty or responds only with error
                                currentBackOff = reconnectBackoff.start()
                                read = true
                            }
                        }
                        onMessage(msg)
                    } catch (t: Throwable) {
                        log.warn("Failed to process WS message. ${t.javaClass}: ${t.message}")
                        Mono.empty()
                    }
                }.onErrorResume { t ->
                    // do not expect any new message processed with the current connection until reconnect
                    active = false
                    log.warn("Connection dropped to $uri. Error: ${t.message}", t)
                    // going to try to reconnect later
                    tryReconnectLater()
                    // completes current outbound flow
                    Mono.empty()
                }

        val calls =
            rpcSend
                .asFlux()
                .map {
                    Unpooled.wrappedBuffer(it.toJson())
                }

        return outbound.send(
            Flux.merge(
                calls.subscribeOn(Schedulers.boundedElastic()),
                consumer.then(Mono.empty<ByteBuf>()).subscribeOn(Schedulers.boundedElastic()),
            ),
        )
    }

    fun onMessage(msg: ResponseWSParser.WsResponse): Mono<Void> =
        Mono
            .fromCallable {
                when (msg.type) {
                    ResponseWSParser.Type.RPC -> onMessageRpc(msg)
                    ResponseWSParser.Type.SUBSCRIPTION -> onMessageSubscription(msg)
                }
            }.doOnError { t -> log.warn("Failed to process WS message. ${t.javaClass}: ${t.message}") }
            .onErrorComplete()
            .then()

    fun onMessageRpc(msg: ResponseWSParser.WsResponse) {
        val rpcResponse =
            JsonRpcResponse(
                msg.value,
                msg.error,
                msg.id,
                200,
                null,
            )
        val sender = currentRequests.remove(msg.id.asNumber().toInt())
        if (sender == null) {
            log.warn("Unknown response received for ${msg.id}")
        } else {
            try {
                val emitResult = sender.tryEmitValue(rpcResponse)
                if (emitResult.isFailure) {
                    log.debug("Response is ignored {}", emitResult)
                }
            } catch (t: Throwable) {
                log.warn("Response is not processed ${t.message}", t)
            }
        }
    }

    fun onMessageSubscription(msg: ResponseWSParser.WsResponse) {
        val subscription =
            JsonRpcWsMessage(
                msg.value,
                msg.error,
                msg.id.asString(),
            )
        val status = subscriptionResponses.tryEmitNext(subscription)
        if (status.isFailure) {
            if (status == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                log.debug("No subscribers to WS response")
            } else {
                log.warn("Failed to proceed with a WS message: $status")
            }
        }
    }

    override fun getSubscribeResponses(): Flux<JsonRpcWsMessage> =
        Flux
            .from(subscriptionResponses.asFlux())
            // make sure to complete the subscription when the connection is closed,
            // because even if it reconnects later a new subscription must be created
            .takeUntilOther(disconnects.asFlux())

    override fun callRpc(originalRequest: JsonRpcRequest): Mono<JsonRpcResponse> {
        // use an internal id sequence, to avoid id conflicts with user calls
        val internalId = sendIdSeq.getAndIncrement()
        return Mono
            .fromCallable {
                val timer = StopWatch.createStarted()
                val originalId = originalRequest.id
                Tuples.of(originalRequest.copy(id = internalId), originalId, timer)
            }.flatMap { request ->
                waitForResponse(request.t1, request.t2, request.t3)
            }.contextWrite(Global.monitoring.ingress.setRpcId(internalId))
    }

    private fun sendRpc(request: JsonRpcRequest) {
        // submit to upstream in a separate thread, to free current thread (needs for subscription, etc)
        sendExecutor.execute {
            val result = rpcSend.tryEmitNext(request)
            if (result.isFailure) {
                log.warn("Failed to send RPC request: $result")
            }
        }
    }

    fun waitForResponse(
        request: JsonRpcRequest,
        originalId: Int,
        timer: StopWatch,
    ): Mono<JsonRpcResponse> {
        val internalId = request.id.toLong()
        val onResponse = Sinks.one<JsonRpcResponse>()
        currentRequests[internalId.toInt()] = onResponse
        rpcMetrics?.onMessageEnqueued()

        // a default response when nothing is received back from WS after a timeout
        val noResponse =
            JsonRpcException(
                JsonRpcResponse.Id.from(originalId),
                JsonRpcError(
                    RpcResponseError.CODE_INTERNAL_ERROR,
                    "Response not received from WebSocket",
                ),
            )

        // Immediate stop if WS got disconnected
        val failOnDisconnect =
            Mono
                .from(disconnects.asFlux())
                .flatMap {
                    Mono.error<JsonRpcResponse>(
                        JsonRpcException(
                            JsonRpcResponse.Id.from(originalId),
                            JsonRpcError(
                                RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                                "Disconnected from WebSocket",
                            ),
                        ),
                    )
                }

        return Mono
            .from(onResponse.asMono())
            .or(failOnDisconnect)
            .doOnSubscribe { sendRpc(request) }
            .take(Defaults.timeout)
            .doOnNext { rpcMetrics?.timer?.record(timer.nanoTime, TimeUnit.NANOSECONDS) }
            .doOnError { rpcMetrics?.fails?.increment() }
            .map { it.copyWithId(JsonRpcResponse.Id.from(originalId)) }
            .switchIfEmpty(
                Mono.fromCallable { log.warn("No response for ${request.method} ${request.params}") }.then(Mono.error(noResponse)),
            ).doFinally {
                currentRequests.remove(internalId.toInt())
                rpcMetrics?.onMessageFinished()
            }
    }

    override fun close() {
        log.info("Closing connection to WebSocket $uri")
        keepConnection = false
        connection?.dispose()
        connection = null
        currentRequests.clear()
    }
}
