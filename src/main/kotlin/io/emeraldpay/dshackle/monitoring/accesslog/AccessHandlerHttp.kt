package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.rpc.NativeCall
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.websocket.WebsocketInbound
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Access Log handler for JSON RPC proxy
 *
 * @see io.emeraldpay.dshackle.proxy.ProxyServer
 */
@Service
class AccessHandlerHttp(
    @Autowired private val mainConfig: MainConfig,
    @Autowired accessLogWriter: AccessLogWriter
) {

    companion object {
        private val log = LoggerFactory.getLogger(AccessHandlerHttp::class.java)

        private val NO_SUBSCRIBE = NoOnSubscriptionHandler()
        private val NO_REQUEST = NoOpHandler()
    }

    /**
     * Use factory since we need a different behaviour for situation when log is configured and when is not
     */
    val factory: HandlerFactory = if (mainConfig.accessLogConfig.enabled) {
        StandardFactory(accessLogWriter)
    } else {
        NoOpFactory()
    }

    interface HandlerFactory {
        fun create(req: HttpServerRequest, blockchain: Chain): RequestHandler
        fun start(req: WebsocketInbound, blockchain: Chain): WsHandlerFactory
    }

    interface WsHandlerFactory {
        fun call(): RequestHandler
        fun subscribe(): SubscriptionHandler
    }

    class NoOpFactory : HandlerFactory {
        override fun create(req: HttpServerRequest, blockchain: Chain): RequestHandler {
            return NO_REQUEST
        }

        override fun start(req: WebsocketInbound, blockchain: Chain): WsHandlerFactory {
            return NO_REQUEST
        }
    }

    class StandardFactory(val accessLogWriter: AccessLogWriter) : HandlerFactory {
        override fun create(req: HttpServerRequest, blockchain: Chain): RequestHandler {
            return StandardHandler(accessLogWriter, req, blockchain)
        }

        override fun start(req: WebsocketInbound, blockchain: Chain): WsHandlerFactory {
            return StandardWsHandlerFactory(accessLogWriter, req, blockchain)
        }
    }

    interface RequestHandler {
        fun close()
        fun onRequest(request: BlockchainOuterClass.NativeCallRequest)
        fun onResponse(callResult: NativeCall.CallResult)
    }

    interface SubscriptionHandler {
        fun onRequest(request: Pair<String, ByteArray?>)
        fun onResponse(msgSize: Long)
    }

    class NoOpHandler : RequestHandler, WsHandlerFactory {
        override fun close() {
        }

        override fun onRequest(request: BlockchainOuterClass.NativeCallRequest) {
        }

        override fun onResponse(callResult: NativeCall.CallResult) {
        }

        override fun call(): RequestHandler {
            return this
        }

        override fun subscribe(): SubscriptionHandler {
            return NO_SUBSCRIBE
        }
    }

    class NoOnSubscriptionHandler : SubscriptionHandler {
        override fun onRequest(request: Pair<String, ByteArray?>) {
        }

        override fun onResponse(msgSize: Long) {
        }
    }

    class StandardWsHandlerFactory(
        private val accessLogWriter: AccessLogWriter,
        private val wsRequest: WebsocketInbound,
        private val blockchain: Chain
    ) : WsHandlerFactory {

        override fun call(): RequestHandler {
            return WsRequestHandler(accessLogWriter, wsRequest, blockchain)
        }

        override fun subscribe(): SubscriptionHandler {
            return WsSubscriptionHandler(accessLogWriter, wsRequest, blockchain)
        }
    }

    abstract class AbstractRequestHandler(
        private val accessLogWriter: AccessLogWriter,
        private val channel: Events.Channel
    ) : RequestHandler {
        protected var startTs: Instant? = null
        protected var request: BlockchainOuterClass.NativeCallRequest? = null
        protected val responses = ArrayList<NativeCall.CallResult>()
        protected val updateLock = ReentrantLock()

        override fun onRequest(request: BlockchainOuterClass.NativeCallRequest) {
            this.startTs = Instant.now()
            this.request = request
        }

        override fun onResponse(callResult: NativeCall.CallResult) {
            updateLock.withLock {
                responses.add(callResult)
            }
        }

        fun onClose(builder: EventsBuilder.NativeCall) {
            val responseTime = Instant.now()
            responses
                .map {
                    builder.onReply(it, channel).also { item ->
                        // since for JSON RPC you get a single response then the timestamp of all items included in it must have the same timestamp
                        item.ts = responseTime
                    }
                }
                .let(accessLogWriter::submit)
        }
    }

    class StandardHandler(
        accessLogWriter: AccessLogWriter,
        private val httpRequest: HttpServerRequest,
        private val blockchain: Chain
    ) : RequestHandler, AbstractRequestHandler(accessLogWriter, Events.Channel.JSONRPC) {

        override fun close() {
            if (request == null) {
                return
            }
            val builder = EventsBuilder.NativeCall(startTs!!)
            builder.withChain(blockchain.id)
            builder.start(httpRequest)
            builder.onRequest(request!!)
            onClose(builder)
        }
    }

    class WsRequestHandler(
        accessLogWriter: AccessLogWriter,
        private val wsRequest: WebsocketInbound,
        private val blockchain: Chain
    ) : RequestHandler, AbstractRequestHandler(accessLogWriter, Events.Channel.WSJSONRPC) {

        override fun close() {
            if (request == null) {
                return
            }
            val builder = EventsBuilder.NativeCall(startTs!!)
            builder.withChain(blockchain.id)
            builder.start(wsRequest)
            builder.onRequest(request!!)
            onClose(builder)
        }
    }

    class WsSubscriptionHandler(
        private val accessLogWriter: AccessLogWriter,
        private val wsRequest: WebsocketInbound,
        private val blockchain: Chain
    ) : SubscriptionHandler {

        private var builder: EventsBuilder.NativeSubscribeHttp? = null

        override fun onRequest(request: Pair<String, ByteArray?>) {
            val builder = EventsBuilder.NativeSubscribeHttp(Events.Channel.WSJSONRPC, blockchain)
            builder.start(wsRequest)
            builder.onRequest(request)
            this.builder = builder
        }

        override fun onResponse(msgSize: Long) {
            builder
                ?.onReply(msgSize)
                ?.let(accessLogWriter::submit)
        }
    }
}
