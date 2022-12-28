package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.JsonRpcReader

interface HttpFactory {
    fun create(id: String?, chain: Chain): JsonRpcReader
}
