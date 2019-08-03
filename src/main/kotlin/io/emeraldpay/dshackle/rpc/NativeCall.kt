package io.emeraldpay.dshackle.rpc

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.ConfiguredUpstreams
import io.emeraldpay.dshackle.upstream.EthereumApi
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import reactor.util.function.Tuples
import java.lang.Exception

@Service
class NativeCall(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val objectMapper: ObjectMapper
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return requestMono.flatMapMany { request ->
            val chain= Chain.byId(request.chain.number)
            if (chain == Chain.UNSPECIFIED) {
                // TODO send error to all requests?
                throw Exception("Invalid chain id: ${request.chain.number}")
            }
            val matcher = Selector.convertToMatcher(request.selector)
            val upstream = upstreams.getUpstream(chain)?.getApi(matcher) ?: throw Exception("Chain ${chain.id} is unavailable")
            request.itemsList.toFlux().map {
                val method = it.target
                val params = it.payload.toStringUtf8()
                CallContext(it.id, upstream, Tuples.of(method, params))
            }
        }
        .map {
            val params = extractParams(it.payload.t2)
            it.withPayload(Tuples.of(it.payload.t1, params))
        }
        .flatMap { ctx ->
            ctx.upstream.execute(ctx.id, ctx.payload.t1, ctx.payload.t2).map { resp ->
                ctx.withPayload(resp)
            }.onErrorMap {
                CallFailure(ctx.id, it)
            }
        }
        .map {
            BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                    .setSucceed(true)
                    .setId(it.id)
                    .setPayload(ByteString.copyFrom(it.payload))
                    .build()
        }
        .onErrorResume() {
            val id: Int = if (it != null && CallFailure::class.isInstance(it)) {
                (it as CallFailure).id
            } else {
                log.error("Lost context for a native call", it)
                0
            }
            BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                    .setSucceed(false)
                    .setId(id)
                    .build()
                    .toMono()
        }
    }

    private fun extractParams(jsonParams: String): List<Any> {
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return req as List<Any>
    }

    private class CallContext<T>(val id: Int, val upstream: EthereumApi, val payload: T) {
        fun <X> withPayload(payload: X): CallContext<X> {
            return CallContext(id, upstream, payload)
        }
    }

    class CallFailure(val id: Int, val reason: Throwable): Exception("Failed to call $id: ${reason.message}")
}