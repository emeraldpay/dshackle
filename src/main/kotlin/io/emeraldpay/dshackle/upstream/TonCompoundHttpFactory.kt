package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.restclient.TonCompoundRestHttpReader

class TonCompoundHttpFactory(
    private val tonHttpFactory: HttpFactory,
    private val tonV3HttpFactory: HttpFactory?,
) : HttpFactory {

    override fun create(id: String?, chain: Chain): HttpReader {
        val tonReader = tonHttpFactory.create(id, chain)
        if (tonV3HttpFactory != null) {
            return TonCompoundRestHttpReader(tonReader, tonV3HttpFactory.create(id, chain))
        }
        return tonReader
    }
}
