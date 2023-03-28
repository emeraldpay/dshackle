package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.monitoring.Channel
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.websocket.WebsocketInbound
import java.time.Instant
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Access Log handler for JSON RPC proxy
 *
 * @see io.emeraldpay.dshackle.proxy.ProxyServer
 */
@Service
class AccessLogHandlerHttp(
    @Autowired private val mainConfig: MainConfig,
    @Autowired accessLogWriter: CurrentAccessLogWriter
) {

    companion object {
        private val log = LoggerFactory.getLogger(AccessLogHandlerHttp::class.java)

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
        fun create(req: HttpServerRequest, blockchain: Chain, requestId: UUID): RequestHandler
        fun start(req: WebsocketInbound, blockchain: Chain): WsHandlerFactory
    }

    interface WsHandlerFactory {
        fun call(requestId: UUID): RequestHandler
        fun subscribe(requestId: UUID): SubscriptionHandler
    }

    class NoOpFactory : HandlerFactory {
        override fun create(req: HttpServerRequest, blockchain: Chain, requestId: UUID): RequestHandler {
            return NO_REQUEST
        }

        override fun start(req: WebsocketInbound, blockchain: Chain): WsHandlerFactory {
            return NO_REQUEST
        }
    }

    class StandardFactory(val accessLogWriter: CurrentAccessLogWriter) : HandlerFactory {
        override fun create(req: HttpServerRequest, blockchain: Chain, requestId: UUID): RequestHandler {
            return StandardHandler(accessLogWriter, req, blockchain, requestId)
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

        override fun call(requestId: UUID): RequestHandler {
            return this
        }

        override fun subscribe(requestId: UUID): SubscriptionHandler {
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
        private val accessLogWriter: CurrentAccessLogWriter,
        private val wsRequest: WebsocketInbound,
        private val blockchain: Chain
    ) : WsHandlerFactory {

        override fun call(requestId: UUID): RequestHandler {
            return WsRequestHandler(accessLogWriter, wsRequest, blockchain, requestId)
        }

        override fun subscribe(requestId: UUID): SubscriptionHandler {
            return WsSubscriptionHandler(accessLogWriter, wsRequest, blockchain, requestId)
        }
    }

    abstract class AbstractRequestHandler(
        private val accessLogWriter: CurrentAccessLogWriter,
        private val channel: Channel
    ) : RequestHandler {
        protected var request: BlockchainOuterClass.NativeCallRequest? = null
        protected val responses = ArrayList<NativeCall.CallResult>()
        protected val updateLock = ReentrantLock()

        override fun onRequest(request: BlockchainOuterClass.NativeCallRequest) {
            this.request = request
        }

        override fun onResponse(callResult: NativeCall.CallResult) {
            updateLock.withLock {
                responses.add(callResult)
            }
        }

        fun onClose(builder: RecordBuilder.NativeCall) {
            val responseTime = Instant.now()
            responses
                .map {
                    builder.onReply(it, channel).also { item ->
                        // since for JSON RPC you get a single response then the timestamp of all items included in it must have the same timestamp
                        item.ts = responseTime
                    }
                }
                .let(accessLogWriter::submitAll)
        }
    }

    class StandardHandler(
        accessLogWriter: CurrentAccessLogWriter,
        private val httpRequest: HttpServerRequest,
        private val blockchain: Chain,
        private val requestId: UUID,
    ) : RequestHandler, AbstractRequestHandler(accessLogWriter, Channel.JSONRPC) {

        override fun close() {
            if (request == null) {
                return
            }
            val builder = RecordBuilder.NativeCall(requestId)
            builder.withChain(blockchain.id)
            builder.start(httpRequest)
            builder.onRequest(request!!)
            onClose(builder)
        }
    }

    class WsRequestHandler(
        accessLogWriter: CurrentAccessLogWriter,
        private val wsRequest: WebsocketInbound,
        private val blockchain: Chain,
        private val requestId: UUID,
    ) : RequestHandler, AbstractRequestHandler(accessLogWriter, Channel.WSJSONRPC) {

        override fun close() {
            if (request == null) {
                return
            }
            val builder = RecordBuilder.NativeCall(requestId)
            builder.withChain(blockchain.id)
            builder.start(wsRequest)
            builder.onRequest(request!!)
            onClose(builder)
        }
    }

    class WsSubscriptionHandler(
        private val accessLogWriter: CurrentAccessLogWriter,
        private val wsRequest: WebsocketInbound,
        private val blockchain: Chain,
        private val requestId: UUID,
    ) : SubscriptionHandler {

        private var builder: RecordBuilder.NativeSubscribeHttp? = null

        override fun onRequest(request: Pair<String, ByteArray?>) {
            val builder = RecordBuilder.NativeSubscribeHttp(Channel.WSJSONRPC, blockchain, requestId)
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
