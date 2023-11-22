package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain

class IndexConfig {
    var items: HashMap<Chain, Index> = HashMap<Chain, Index>()

    class Index(
        var rpc: String,
        var store: String,
        var ram_limit: Long?,
    )

    fun isChainEnabled(chain: Chain): Boolean {
        return items.containsKey(chain)
    }

    fun getByChain(chain: Chain): Index? {
        return items.get(chain)
    }
}
