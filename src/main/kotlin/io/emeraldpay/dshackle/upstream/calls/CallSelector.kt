package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import reactor.core.publisher.Mono

interface CallSelector {

    fun getMatcher(method: String, params: String, head: Head, passthrough: Boolean): Mono<Selector.Matcher>
}
