package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.lang.IllegalStateException
import java.time.Duration

class ChainUpstreams (
        val chain: Chain,
        private val upstreams: MutableList<Upstream>,
        targets: EthereumTargets
) : AggregatedUpstreams(targets) {

    private val log = LoggerFactory.getLogger(ChainUpstreams::class.java)
    private var seq = 0
    private var head: EthereumHead?
    private var lagObserver: HeadLagObserver? = null

    init {
        head = updateHead()
        observeStatus()
                .distinctUntilChanged()
                .subscribe { printStatus() }
    }

    internal fun updateHead(): EthereumHead {
        val current = head
        if (current != null && Closeable::class.java.isAssignableFrom(current.javaClass)) {
            (current as Closeable).close()
        }
        lagObserver?.close()
        lagObserver = null
        return if (upstreams.size == 1) {
            val upstream = upstreams.first()
            upstream.setLag(0)
            upstream.getHead()
        } else {
            val newHead = EthereumHeadMerge(upstreams.map { it.getHead() })
            val lagObserver = HeadLagObserver(newHead, upstreams)
            lagObserver.start()
            this.lagObserver = lagObserver
            newHead
        }
    }

    override fun getAll(): List<Upstream> {
        return upstreams
    }

    override fun addUpstream(upstream: Upstream) {
        upstreams.add(upstream)
        head = updateHead()
    }

    override fun getApis(matcher: Selector.Matcher): Iterator<EthereumApi> {
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return FilteringApiIterator(upstreams, i, matcher)
    }

    override fun getApi(matcher: Selector.Matcher): EthereumApi {
        return getApis(matcher).next()
    }

    override fun getHead(): EthereumHead {
        return head!!
    }

    override fun setLag(lag: Long) {
    }

    override fun getLag(): Long {
        return 0
    }

    fun printStatus() {
        var height: Long? = null
        try {
            height = head!!.getHead().block(Duration.ofSeconds(1))?.number
        } catch (e: IllegalStateException) {
            //timout
        } catch (e: Exception) {
            log.warn("Head processing error: ${e.javaClass} ${e.message}")
        }
        val statuses = upstreams.map { it.getStatus() }
                .groupBy { it }
                .map { "${it.key.name}/${it.value.size}" }
                .joinToString(",")
        val lag = upstreams.map { it.getLag() }
                .joinToString(", ")

        log.info("State of ${chain.chainCode}: height=${height ?: '?'}, status=$statuses, lag=[$lag]")
    }


}