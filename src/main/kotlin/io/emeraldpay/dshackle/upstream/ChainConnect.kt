package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.time.Duration

class ChainConnect(
        val chain: Chain,
        val upstreams: List<Upstream>
        ) {

    private val log = LoggerFactory.getLogger(ChainConnect::class.java)
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

    fun printStatus() {
        var height: Long = -1
        try {
            height = head.getHead().block(Duration.ofSeconds(1))?.number ?: -1
        } catch (e: Exception) { }
        val statuses = upstreams.map { it.getStatus() }
                .groupBy { it }
                .map { "${it.key.name}/${it.value.size}" }
                .joinToString(",")

        log.info("State of ${chain.chainCode}: height=$height, status=$statuses")
    }

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
                    return api.api
                }
            }
            throw IllegalStateException("No upstream API available")
        }

    }


}