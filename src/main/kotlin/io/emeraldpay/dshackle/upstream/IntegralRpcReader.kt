package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import reactor.core.publisher.Mono

class IntegralRpcReader(
    verify: DshackleRpcReader,
    hardcoded: DshackleRpcReader,
    ingress: DshackleRpcReader,
) : DshackleRpcReader {
    private val compound =
        CompoundReader(
            verify,
            hardcoded,
            ingress,
        )

    override fun read(key: DshackleRequest): Mono<DshackleResponse> = compound.read(key)
}
