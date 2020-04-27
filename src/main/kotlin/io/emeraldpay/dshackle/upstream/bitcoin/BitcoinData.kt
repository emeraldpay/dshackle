package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.Head
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle

open class BitcoinData(
        api: DirectBitcoinApi,
        head: Head
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinData::class.java)
    }

    private val mempool = CachingMempoolData(api, head)

    open fun getMempool(): CachingMempoolData {
        return mempool
    }

    override fun isRunning(): Boolean {
        return mempool.isRunning
    }

    override fun start() {
        mempool.start()
    }

    override fun stop() {
        mempool.stop()
    }
}