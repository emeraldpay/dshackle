package io.emeraldpay.dshackle.rpc

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.ConfiguredUpstreams
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
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

    open fun nativeCall(request: BlockchainOuterClass.NativeCallRequest, responseObserver: StreamObserver<BlockchainOuterClass.NativeCallReplyItem>) {
        val chain= Chain.byId(request.chain.number)
        if (chain == Chain.UNSPECIFIED) {
            throw Exception("Invalid chain id: ${request.chain.number}")
        }
        val upstream = upstreams.getUpstream(chain)?.getApi() ?: throw Exception("Chain ${chain.id} is unavailable")
        request.itemsList.toFlux()
                .map {
                    val method = it.target
                    val params = it.payload.toStringUtf8()
                    return@map CallContext(it.id, Tuples.of(method, params))
                }
                .map {
                    val params = extractParams(it.payload.t2)
                    return@map it.withPayload(Tuples.of(it.payload.t1, params))
                }
                .flatMap { ctx ->
                    upstream.execute(ctx.id, ctx.payload.t1, ctx.payload.t2).map { resp ->
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
                    return@onErrorResume BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                            .setSucceed(false)
                            .setId(id)
                            .build()
                            .toMono()
                }
                .doOnComplete {
                    responseObserver.onCompleted()
                }
                .subscribe {
                    responseObserver.onNext(it)
                }
    }

    private fun extractParams(jsonParams: String): List<Any> {
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return req as List<Any>
    }

    private class CallContext<T>(val id: Int, val payload: T) {
        fun <X> withPayload(payload: X): CallContext<X> {
            return CallContext(id, payload)
        }
    }

    class CallFailure(val id: Int, val reason: Throwable): Exception("Failed to call $id: ${reason.message}")
}