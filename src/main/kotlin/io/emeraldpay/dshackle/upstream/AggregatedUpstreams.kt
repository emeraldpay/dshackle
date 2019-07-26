package io.emeraldpay.dshackle.upstream

abstract class AggregatedUpstreams {

    abstract fun getAll(): List<Upstream>
    abstract fun addUpstream(upstream: Upstream)
    abstract fun getApis(quorum: Int): Iterator<EthereumApi>
    abstract fun getApi(): EthereumApi
    abstract fun getHead(): EthereumHead

    class SingleApi(
            private val quorumApi: QuorumApi
    ): Iterator<EthereumApi> {

        private var consumed = false

        override fun hasNext(): Boolean {
            return !consumed && quorumApi.hasNext()
        }

        override fun next(): EthereumApi {
            consumed = true
            return quorumApi.next()
        }
    }

    class QuorumApi(
            private val apis: List<Upstream>,
            private val quorum: Int,
            private var pos: Int
    ): Iterator<EthereumApi> {

        private var consumed = 0

        override fun hasNext(): Boolean {
            return consumed < quorum
        }

        override fun next(): EthereumApi {
            val start = pos
            while (pos < start + apis.size) {
                val api = apis[pos++ % apis.size]
                if (api.isAvailable()) {
                    consumed++
                    return api.getApi()
                }
            }
            throw IllegalStateException("No upstream API available")
        }

    }
}