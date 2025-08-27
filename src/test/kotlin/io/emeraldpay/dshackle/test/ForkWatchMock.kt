package io.emeraldpay.dshackle.test

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.NeverForkChoice
import io.emeraldpay.dshackle.upstream.Upstream
import reactor.core.publisher.Flux

class ForkWatchMock(
    val results: Flux<Boolean>,
) : ForkWatch(NeverForkChoice(), Chain.ETHEREUM) {
    override fun register(upstream: Upstream): Flux<Boolean> = results
}
