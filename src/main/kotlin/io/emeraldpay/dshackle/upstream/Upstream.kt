package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import org.slf4j.LoggerFactory

class Upstream(
        val chain: Chain,
        val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null
) {

    private val log = LoggerFactory.getLogger(Upstream::class.java)

    val head: EthereumHead = if (ethereumWs != null) {
        EthereumWsHead(ethereumWs)
    } else {
        EthereumRpcHead(api).apply {
            this.start()
        }
    }

    init {
        log.info("Configured for ${chain.chainName}")
    }
}