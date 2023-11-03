package io.emeraldpay.dshackle.upstream.polkadot

import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector.Matcher
import io.emeraldpay.dshackle.upstream.calls.DefaultPolkadotMethods
import io.emeraldpay.dshackle.upstream.generic.GenericUpstream
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler

class PolkadotEgressSubscription(
    val upstream: Multistream,
    val scheduler: Scheduler,
) : EgressSubscription {
    override fun getAvailableTopics(): List<String> {
        return DefaultPolkadotMethods.subs.map { it.first }
    }

    override fun subscribe(topic: String, params: Any?, matcher: Matcher): Flux<ByteArray> {
        val up = upstream.getUpstreams().shuffled().first { matcher.matches(it) } as GenericUpstream
        return up.getIngressSubscription().get<ByteArray>(topic)?.connect(matcher) ?: Flux.empty()
    }
}
