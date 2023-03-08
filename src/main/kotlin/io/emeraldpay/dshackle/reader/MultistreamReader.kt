package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.quorum.NotLaggingQuorum
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

open class MultistreamReader(
    private val upstreams: Multistream,
) : DshackleRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(MultistreamReader::class.java)
    }

    private val quorumReaderFactory = QuorumReaderFactory.default()

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        val quorum = upstreams.getMethods().createQuorumFor(key.method).also {
            it.init(upstreams.getHead())
        }

        // for NotLaggingQuorum it makes sense to select compatible upstreams before the call
        val matcher: Selector.Matcher = if (quorum is NotLaggingQuorum) {
            val lag = quorum.maxLag
            val minHeight = ((upstreams.getHead().getCurrentHeight() ?: 0) - lag).coerceAtLeast(0)
            val heightMatcher = Selector.HeightMatcher(minHeight)
            Selector.MultiMatcher(listOf(heightMatcher, key.matcher))
        } else {
            key.matcher
        }

        val apis = upstreams.getApiSource(matcher)

        val reader = quorumReaderFactory.create(apis, quorum, null)
        return reader.read(key.asJsonRequest())
            .map {
                DshackleResponse(
                    id = key.id,
                    result = it.value,
                    error = null
                )
            }
    }
}
