package io.emeraldpay.dshackle.upstream

class FilteringApiIterator(
        private val apis: List<Upstream>,
        private var pos: Int,
        private val matcher: Selector.Matcher,
        private val repeatLimit: Int = 3
): Iterator<EthereumApi> {

    private var nextApi: Upstream? = null
    private var consumed = 0

    private fun nextInternal(): Boolean {
        if (nextApi != null) {
            return true
        }
        while (nextApi == null) {
            consumed++
            if (consumed > apis.size * repeatLimit) {
                return false
            }
            val api = apis[pos++ % apis.size]
            if (api.isAvailable(matcher)) {
                nextApi = api
            }
        }
        return nextApi != null
    }

    override fun hasNext(): Boolean {
        return nextInternal()
    }

    override fun next(): EthereumApi {
        if (nextInternal()) {
            val curr = nextApi!!
            nextApi = null
            return curr.getApi(matcher)
        }
        throw IllegalStateException("No upstream API available")
    }
}