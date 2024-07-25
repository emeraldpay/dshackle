package io.emeraldpay.dshackle.upstream

class ChainCallUpstreamException(
    id: ChainResponse.Id,
    error: ChainCallError,
) : ChainException(id, error, emptyList(), false)
