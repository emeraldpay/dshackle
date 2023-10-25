package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.JsonRpcHttpReader

interface HttpFactory {
    fun create(id: String?, chain: Chain): JsonRpcHttpReader
}
