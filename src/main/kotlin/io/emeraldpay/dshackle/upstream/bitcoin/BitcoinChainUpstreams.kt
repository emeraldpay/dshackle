package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle

class BitcoinChainUpstreams(
        chain: Chain,
        val upstreams: MutableList<BitcoinUpstream>,
        caches: Caches,
        objectMapper: ObjectMapper
) : ChainUpstreams<DirectBitcoinApi>(chain, upstreams as MutableList<Upstream<DirectBitcoinApi>>, caches, objectMapper) {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinChainUpstreams::class.java)
    }

    private var head: Head? = null

    override fun init() {
        if (upstreams.size > 0) {
            head = updateHead()
        }
        super.init()
    }

    override fun updateHead(): Head {
        head?.let {
            if (it is Lifecycle) {
                it.stop()
            }
        }
        lagObserver?.stop()
        lagObserver = null
        val head = if (upstreams.size == 1) {
            val upstream = upstreams.first()
            upstream.setLag(0)
            upstream.getHead()
        } else {
            val newHead = MergedHead(upstreams.map { it.getHead() }).apply {
                this.start()
            }
//            val lagObserver = TODO
//            this.lagObserver = lagObserver
            newHead
        }
        onHeadUpdated(head)
        return head
    }

    override fun setHead(head: Head) {
        this.head = head
    }

    override fun getHead(): Head {
        return head!!
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return upstreams.flatMap { it.getLabels() }
    }

    override fun <A : UpstreamApi> castApi(apiType: Class<A>): Upstream<A> {
        if (!apiType.isAssignableFrom(DirectBitcoinApi::class.java)) {
            throw ClassCastException("Cannot cast ${EthereumApi::class.java} to $apiType")
        }
        return this as Upstream<A>
    }

    override fun <T : Upstream<TA>, TA : UpstreamApi> cast(selfType: Class<T>, apiType: Class<TA>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return castApi(apiType) as T
    }

}