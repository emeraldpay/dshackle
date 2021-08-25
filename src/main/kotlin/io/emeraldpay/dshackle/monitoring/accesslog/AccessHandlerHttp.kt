package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.netty.http.server.HttpServerRequest
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
    }

    class NoOpFactory() : HandlerFactory {
        override fun create(req: HttpServerRequest, blockchain: Chain): RequestHandler {
            return NoOpHandler()
        }
    }

    class StandardFactory(val accessLogWriter: AccessLogWriter) : HandlerFactory {
        override fun create(req: HttpServerRequest, blockchain: Chain): RequestHandler {
            return StandardHandler(accessLogWriter, req, blockchain)
        }
    }

    interface RequestHandler {
        fun close()
        fun onRequest(request: BlockchainOuterClass.NativeCallRequest)
        fun onResponse(callResult: NativeCall.CallResult)
    }

    class NoOpHandler : RequestHandler {
        override fun close() {
        }

        override fun onRequest(request: BlockchainOuterClass.NativeCallRequest) {
        }

        override fun onResponse(callResult: NativeCall.CallResult) {
        }
    }

    class StandardHandler(
            private val accessLogWriter: AccessLogWriter,
            private val httpRequest: HttpServerRequest,
            private val blockchain: Chain
    ) : RequestHandler {

        private var request: BlockchainOuterClass.NativeCallRequest? = null
        private val responses = ArrayList<NativeCall.CallResult>()
        private val updateLock = ReentrantLock()

        override fun close() {
            if (request == null) {
                return
            }
            val responseTime = Instant.now()
            val builder = EventsBuilder.NativeCall()
            builder.withChain(blockchain.id)
            builder.start(httpRequest)
            builder.onRequest(request!!)
            responses
                    .map {
                        builder.onReply(it, Events.Channel.JSONRPC).also { item ->
                            //since for JSON RPC you get a single response then the timestamp of all items included in it must have the same timestamp
                            item.ts = responseTime
                        }
                    }
                    .let(accessLogWriter::submit)
        }

        override fun onRequest(request: BlockchainOuterClass.NativeCallRequest) {
            this.request = request
        }

        override fun onResponse(callResult: NativeCall.CallResult) {
            updateLock.withLock {
                responses.add(callResult)
            }
        }
    }
}