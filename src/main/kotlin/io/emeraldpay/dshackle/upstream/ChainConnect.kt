package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import java.lang.IllegalStateException

class ChainConnect(
        val chain: Chain,
        val upstreams: List<Upstream>
        ) {

    private var seq = 0

    val head: EthereumHead = if (upstreams.size == 1) {
        upstreams.first().head
    } else {
        EthereumHeadMerge(upstreams.map { it.head })
    }

    val api: EthereumApi
        get() {
            return getApis(1).next()
        }

    fun getApis(quorum: Int): Iterator<EthereumApi> {
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return QuorumApi(upstreams, 1, seq)
    }

    class SingleApi(
            val quorumApi: QuorumApi
    ): Iterator<EthereumApi> {

        var consumed = false

        override fun hasNext(): Boolean {
            return !consumed && quorumApi.hasNext()
        }

        override fun next(): EthereumApi {
            consumed = true
            return quorumApi.next()
        }
    }

    class QuorumApi(
            val apis: List<Upstream>,
            val quorum: Int,
            var pos: Int
    ): Iterator<EthereumApi> {

        var consumed = 0

        override fun hasNext(): Boolean {
            return consumed < quorum
        }

        override fun next(): EthereumApi {
            val start = pos
            while (pos < start + apis.size) {
                val api = apis[pos++]
                if (api.isAvailable()) {
                    consumed++
                    return api.api
                }
            }
            throw IllegalStateException("No upstream API available")
        }

    }


}