package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.lang.IllegalStateException
import java.time.Duration

class ChainUpstreams (
        val chain: Chain,
        private val upstreams: MutableList<Upstream>
) : AggregatedUpstreams() {

    private val log = LoggerFactory.getLogger(ChainUpstreams::class.java)
    private var seq = 0
    private var head: EthereumHead?

    init {
        head = updateHead()
    }

    internal fun updateHead(): EthereumHead {
        val current = head
        if (current != null && Closeable::class.java.isAssignableFrom(current.javaClass)) {
            (current as Closeable).close()
        }
        return if (upstreams.size == 1) {
            upstreams.first().getHead()
        } else {
            EthereumHeadMerge(upstreams.map { it.getHead() })
        }
    }

    override fun getAll(): List<Upstream> {
        return upstreams
    }

    override fun addUpstream(upstream: Upstream) {
        upstreams.add(upstream)
        head = updateHead()
    }

    override fun getApis(quorum: Int): Iterator<EthereumApi> {
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return QuorumApi(upstreams, 1, seq)
    }

    override fun getApi(): EthereumApi {
        return getApis(1).next()
    }

    override fun getHead(): EthereumHead {
        return head!!
    }

    fun printStatus() {
        var height: Long = -1
        try {
            height = head!!.getHead().block(Duration.ofSeconds(1))?.number ?: -1
        } catch (e: IllegalStateException) {
            //timout
        } catch (e: Exception) {
            log.warn("Head processing error: ${e.javaClass} ${e.message}")
        }
        val statuses = upstreams.map { it.getStatus() }
                .groupBy { it }
                .map { "${it.key.name}/${it.value.size}" }
                .joinToString(",")

        log.info("State of ${chain.chainCode}: height=$height, status=$statuses")
    }


}