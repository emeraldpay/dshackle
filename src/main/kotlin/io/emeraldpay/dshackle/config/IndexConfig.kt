package io.emeraldpay.dshackle.config

import io.emeraldpay.dshackle.Chain
import org.springframework.util.unit.DataSize

class IndexConfig {
    var items: HashMap<Chain, Index> = HashMap<Chain, Index>()

    class Index(
        var store: String,
        var limit: Long?,
    )

    fun getByChain(chain: Chain): Index? {
        return items.get(chain)
    }
}
