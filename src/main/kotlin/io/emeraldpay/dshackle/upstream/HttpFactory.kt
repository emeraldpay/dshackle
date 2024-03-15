package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain

interface HttpFactory {
    fun create(id: String?, chain: Chain): HttpReader
}
