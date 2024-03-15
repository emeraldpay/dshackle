package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.commons.BROADCAST_READER
import io.emeraldpay.dshackle.commons.SPAN_REQUEST_UPSTREAM_ID
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicInteger

class BroadcastReader(
    private val upstreams: List<Upstream>,
    matcher: Selector.Matcher,
    signer: ResponseSigner?,
    private val quorum: CallQuorum,
    private val tracer: Tracer,
) : RequestReader(signer) {
    private val internalMatcher = Selector.MultiMatcher(
        listOf(Selector.AvailabilityMatcher(), matcher),
    )

    companion object {
        private val log = LoggerFactory.getLogger(BroadcastReader::class.java)
    }

    override fun attempts(): AtomicInteger {
        return AtomicInteger(1)
    }

    override fun read(key: ChainRequest): Mono<Result> {
        return Flux.fromIterable(upstreams)
            .filter { internalMatcher.matches(it) }
            .flatMap { up ->
                execute(key, up)
            }.map {
                if (it.jsonRpcResponse.hasResult()) {
                    val sig = getSignature(key, it.jsonRpcResponse, it.upstream.getId())
                    quorum.record(it.jsonRpcResponse, sig, it.upstream)
                } else {
                    val err = ChainException(ChainResponse.NumberId(key.id), it.jsonRpcResponse.error!!, it.upstream.getId())
                    quorum.record(err, null, it.upstream)
                }
                quorum
            }.onErrorResume { err ->
                log.error("Broadcast error: ${err.message}")
                Mono.error(handleError(null, 0, null))
            }.collectList()
            .flatMap {
                if (quorum.isResolved()) {
                    val res = Result(
                        quorum.getResponse()!!.getResult(),
                        quorum.getSignature(),
                        upstreams.size,
                        quorum.getResolvedBy().first(),
                        null,
                    )
                    Mono.just(res)
                } else {
                    Mono.error(handleError(quorum.getError(), key.id, null))
                }
            }
    }

    private fun execute(
        key: ChainRequest,
        upstream: Upstream,
    ): Mono<BroadcastResponse> =
        SpannedReader(
            upstream.getIngressReader(),
            tracer,
            BROADCAST_READER,
            mapOf(SPAN_REQUEST_UPSTREAM_ID to upstream.getId()),
        )
            .read(key)
            .map { BroadcastResponse(it, upstream) }
            .onErrorResume {
                log.warn("Error during execution ${key.method} from upstream ${upstream.getId()} with message -  ${it.message}")
                Mono.just(
                    BroadcastResponse(ChainResponse(null, getError(key, it).error), upstream),
                )
            }

    private class BroadcastResponse(
        val jsonRpcResponse: ChainResponse,
        val upstream: Upstream,
    )
}
