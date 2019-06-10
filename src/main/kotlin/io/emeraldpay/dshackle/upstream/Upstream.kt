package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson

class Upstream(
        val chain: Chain,
        val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null
) {

    val head: EthereumHead = if (ethereumWs != null) {
        EthereumWsHead(ethereumWs)
    } else {
        EthereumRpcHead(api).apply {
            this.start()
        }
    }

    init {

    }
}