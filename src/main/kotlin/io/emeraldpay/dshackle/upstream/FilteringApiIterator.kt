package io.emeraldpay.dshackle.upstream

class FilteringApiIterator(
        private val upstreams: List<Upstream>,
        private var pos: Int,
        private val matcher: Selector.Matcher,
        private val repeatLimit: Int = 3
): Iterator<DirectEthereumApi> {

    private var nextUpstream: Upstream? = null
    private var consumed = 0

    private fun nextInternal(): Boolean {
        if (nextUpstream != null) {
            return true
        }
        while (nextUpstream == null) {
            consumed++
            if (consumed > upstreams.size * repeatLimit) {
                return false
            }
            val upstream = upstreams[pos++ % upstreams.size]
            if (upstream.isAvailable(matcher)) {
                nextUpstream = upstream
            }
        }
        return nextUpstream != null
    }

    override fun hasNext(): Boolean {
        return nextInternal()
    }

    override fun next(): DirectEthereumApi {
        if (nextInternal()) {
            val curr = nextUpstream!!
            nextUpstream = null
            return curr.getApi(matcher)
        }
        throw IllegalStateException("No upstream API available")
    }
}