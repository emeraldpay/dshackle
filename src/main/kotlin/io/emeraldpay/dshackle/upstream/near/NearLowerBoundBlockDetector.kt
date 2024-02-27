package io.emeraldpay.dshackle.upstream.near

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Mono

class NearLowerBoundBlockDetector(
    chain: Chain,
    val upstream: Upstream,
) : LowerBoundBlockDetector(chain, upstream) {

    override fun lowerBlockDetect(): Mono<LowerBlockData> {
        return upstream.getIngressReader().read(JsonRpcRequest("status", ListParams())).map {
            val resp = Global.objectMapper.readValue(it.getResult(), NearStatus::class.java)
            LowerBlockData(resp.syncInfo.earliestHeight, null, resp.syncInfo.earliestBlockTime.toEpochMilli())
        }
    }

    override fun periodRequest(): Long {
        return 3
    }
}
