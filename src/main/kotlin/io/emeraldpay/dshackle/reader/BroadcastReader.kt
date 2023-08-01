package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.commons.BROADCAST_READER
import io.emeraldpay.dshackle.commons.SPAN_REQUEST_UPSTREAM_ID
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicInteger

class BroadcastReader(
    private val upstreams: List<Upstream>,
    matcher: Selector.Matcher,
    signer: ResponseSigner?,
    private val tracer: Tracer
) : RpcReader(signer) {
    private val internalMatcher = Selector.MultiMatcher(
        listOf(Selector.AvailabilityMatcher(), matcher)
    )

    companion object {
        private val log = LoggerFactory.getLogger(BroadcastReader::class.java)
    }

    override fun attempts(): AtomicInteger {
        return AtomicInteger(1)
    }

    override fun read(key: JsonRpcRequest): Mono<Result> {
        return Mono.just(upstreams)
            .map { ups ->
                ups.filter { internalMatcher.matches(it) }.map { execute(key, it) }
            }.flatMap {
                Mono.zip(it) { responses ->
                    analyzeResponses(
                        key,
                        getJsonRpcResponses(key.method, responses)
                    )
                }.onErrorResume { err ->
                    log.error("Broadcast error: ${err.message}")
                    Mono.error(handleError(null, 0, null))
                }.flatMap { broadcastResult ->
                    if (broadcastResult.result != null) {
                        Mono.just(
                            Result(broadcastResult.result, broadcastResult.signature, 0, null)
                        )
                    } else {
                        val err = handleError(broadcastResult.error, key.id, null)
                        Mono.error(err)
                    }
                }
            }
    }

    private fun analyzeResponses(key: JsonRpcRequest, jsonRpcResponses: List<BroadcastResponse>): BroadcastResult {
        val errors = mutableListOf<JsonRpcError>()
        jsonRpcResponses.forEach {
            val response = it.jsonRpcResponse
            if (response.hasResult()) {
                val signature = getSignature(key, response, it.upstreamId)
                return BroadcastResult(response.getResult(), null, signature)
            } else if (response.hasError()) {
                errors.add(response.error!!)
            }
        }

        val error = errors.takeIf { it.isNotEmpty() }?.get(0)

        return BroadcastResult(error)
    }

    private fun getJsonRpcResponses(method: String, responses: Array<Any>) =
        responses
            .map { response ->
                (response as BroadcastResponse)
                    .also { r ->
                        if (r.jsonRpcResponse.hasResult()) {
                            log.info(
                                "Response for $method from upstream ${r.upstreamId}: ${String(r.jsonRpcResponse.getResult())}"
                            )
                        }
                    }
            }

    private fun execute(
        key: JsonRpcRequest,
        upstream: Upstream
    ): Mono<BroadcastResponse> =
        SpannedReader(
            upstream.getIngressReader(), tracer, BROADCAST_READER, mapOf(SPAN_REQUEST_UPSTREAM_ID to upstream.getId())
        )
            .read(key)
            .map { BroadcastResponse(it, upstream.getId()) }
            .onErrorResume {
                log.warn("Error during execution ${key.method} from upstream ${upstream.getId()} with message -  ${it.message}")
                Mono.just(
                    BroadcastResponse(JsonRpcResponse(null, getError(key, it).error), upstream.getId())
                )
            }

    private class BroadcastResponse(
        val jsonRpcResponse: JsonRpcResponse,
        val upstreamId: String
    )

    private class BroadcastResult(
        val result: ByteArray?,
        val error: JsonRpcError?,
        val signature: ResponseSigner.Signature?
    ) {
        constructor(error: JsonRpcError?) : this(null, error, null)
    }
}
