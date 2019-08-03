package io.emeraldpay.dshackle.upstream

class FilteringApiIterator(
        private val apis: List<Upstream>,
        private val quorum: Int,
        private var pos: Int,
        private val matcher: Selector.Matcher
): Iterator<EthereumApi> {

    private var consumed = 0

    override fun hasNext(): Boolean {
        return consumed < quorum
    }

    override fun next(): EthereumApi {
        val start = pos
        while (pos < start + apis.size) {
            val api = apis[pos++ % apis.size]
            if (api.isAvailable(matcher)) {
                consumed++
                return api.getApi(matcher)
            }
        }
        throw IllegalStateException("No upstream API available")
    }
}