package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

open class MultistreamReader(
    private val upstreams: Multistream,
    private val signer: ResponseSigner,
) : DshackleRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(MultistreamReader::class.java)
    }

    /**
     * Matchers that should be always applied to a request
     */
    private val alwaysRequiredMatchers = listOf(
        // Since DshackleRequest is always a RPC request require its support
        Selector.CapabilityMatcher(Capability.RPC)
    )
    private val quorumReaderFactory = QuorumReaderFactory.default()

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        val quorum = upstreams.getMethods().createQuorumFor(key.method).also {
            it.init(upstreams.getHead())
        }
        val matcher: Selector.Matcher = createMatcher(key, quorum)

        val apis = upstreams.getApiSource(matcher)

        val reader = quorumReaderFactory.create(apis, quorum, signer)
        return reader.read(key.asJsonRequest())
            .map {
                DshackleResponse(
                    id = key.id,
                    result = it.value,
                    error = null,
                    providedSignature = it.signature
                )
            }
    }

    open fun createMatcher(key: DshackleRequest, quorum: CallQuorum): Selector.Matcher {
        // make sure we select only upstream that provides the method
        val selectUpstreamWithMethod = Selector.MethodMatcher(key.method)

        // for NotLaggingQuorum it makes sense to select compatible upstreams before the call
        val rightHeight: List<Selector.Matcher> = if (quorum is NotLaggingQuorum) {
            val lag = quorum.maxLag
            val minHeight = ((upstreams.getHead().getCurrentHeight() ?: 0) - lag).coerceAtLeast(0)
            val heightMatcher = Selector.HeightMatcher(minHeight)
            listOf(heightMatcher)
        } else {
            emptyList()
        }

        return Selector.MultiMatcher(
            alwaysRequiredMatchers +
                selectUpstreamWithMethod +
                rightHeight +
                key.matcher
        )
    }
}
