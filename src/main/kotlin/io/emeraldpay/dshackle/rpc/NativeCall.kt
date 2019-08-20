package io.emeraldpay.dshackle.rpc

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.grpc.Chain
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.lang.Exception
import java.time.Duration
import java.util.function.Predicate

@Service
class NativeCall(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val objectMapper: ObjectMapper
) {

    private val log = LoggerFactory.getLogger(NativeCall::class.java)

    open fun nativeCall(requestMono: Mono<BlockchainOuterClass.NativeCallRequest>): Flux<BlockchainOuterClass.NativeCallReplyItem> {
        return requestMono.flatMapMany(this::prepareCall)
            .map(this::setupCallParams)
            .parallel()
            .flatMap(this::executeOnRemote)
            .sequential()
            .map(this::buildResponse)
            .doOnError { e -> log.warn("Error during native call", e) }
            .onErrorResume(this::processException)
    }

    fun setupCallParams(it: CallContext<Tuple2<String, String>>): CallContext<Tuple2<String, List<Any>>> {
        val params = extractParams(it.payload.t2)
        return it.withPayload(Tuples.of(it.payload.t1, params))
    }

    fun buildResponse(it: CallContext<ByteArray>): BlockchainOuterClass.NativeCallReplyItem {
        return BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                .setSucceed(true)
                .setId(it.id)
                .setPayload(ByteString.copyFrom(it.payload))
                .build()
    }

    fun processException(it: Throwable?): Mono<BlockchainOuterClass.NativeCallReplyItem> {
        val id: Int = if (it != null && CallFailure::class.isInstance(it)) {
            (it as CallFailure).id
        } else {
            log.error("Lost context for a native call", it)
            0
        }
        return BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                .setSucceed(false)
                .setId(id)
                .build()
                .toMono()
    }

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest): Flux<CallContext<Tuple2<String, String>>> {
        val chain = Chain.byId(request.chain.number)
        if (chain == Chain.UNSPECIFIED) {
            return Flux.error<CallContext<Tuple2<String, String>>>(CallFailure(0, Exception("Invalid chain id: ${request.chain.number}")))
        }
        val upstream = upstreams.getUpstream(chain)
                ?: return Flux.error<CallContext<Tuple2<String, String>>>(CallFailure(0, Exception("Chain ${chain.id} is unavailable")))

        return prepareCall(request, upstream)
    }

    fun prepareCall(request: BlockchainOuterClass.NativeCallRequest, upstream: AggregatedUpstream): Flux<CallContext<Tuple2<String, String>>> {
        val matcher = Selector.convertToMatcher(request.selector)
        val apis = upstream.getApis(matcher)
        return request.itemsList.toFlux().map {
            val method = it.method
            val params = it.payload.toStringUtf8()
            val callQuorum = upstream.targets?.getQuorumFor(method) ?: AlwaysQuorum()
            callQuorum.init(upstream.getHead())

            CallContext(it.id, apis, callQuorum, Tuples.of(method, params))
        }
    }

    fun executeOnRemote(ctx: CallContext<Tuple2<String, List<Any>>>): Mono<CallContext<ByteArray>> {
        val p: Predicate<Any> = CallQuorum.untilResolved(ctx.callQuorum)
        val all = ctx.apis.toFlux().share()
        //execute on the first API immediately, and then make a delay between each call to not dos upstreams
        val immediate = Flux.from(all).take(1)
        val retries = Flux.from(all).delayElements(Duration.ofMillis(200))
        return Flux.concat(immediate, retries)
                .takeWhile(p)
                .flatMap { api ->
                    api.execute(ctx.id, ctx.payload.t1, ctx.payload.t2).map { Tuples.of(it, api.upstream!!) }
                }
                .reduce(ctx.callQuorum, CallQuorum.asReducer())
                .filter { it.isResolved() }
                .map {
                    val result  = it.getResult()
                            ?: throw CallFailure(ctx.id, Exception("No response from upstream for ${ctx.payload.t1}"))
                    ctx.withPayload(result)
                }
                .onErrorMap {
                    log.error("Failed to make a call", it)
                    if (it is CallFailure) it
                    else CallFailure(ctx.id, it)
                }
                .switchIfEmpty(
                        Mono.error<CallContext<ByteArray>>(CallFailure(ctx.id, Exception("No response or no available upstream for ${ctx.payload.t1}")))
                )
    }

    private fun extractParams(jsonParams: String): List<Any> {
        if (StringUtils.isEmpty(jsonParams)) {
            return emptyList()
        }
        val req = objectMapper.readValue(jsonParams, List::class.java)
        return req as List<Any>
    }

    open class CallContext<T>(val id: Int, val apis: Iterator<EthereumApi>, val callQuorum: CallQuorum, val payload: T) {
        fun <X> withPayload(payload: X): CallContext<X> {
            return CallContext(id, apis, callQuorum, payload)
        }
    }

    open class CallFailure(val id: Int, val reason: Throwable): Exception("Failed to call $id: ${reason.message}")
}