package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector.Matcher
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler

class GenericEgressSubscription(
    val multistream: Multistream,
    val scheduler: Scheduler,
) : EgressSubscription {
    override fun getAvailableTopics(): List<String> {
        return multistream.getUpstreams()
            .flatMap { (it as GenericUpstream).getIngressSubscription().getAvailableTopics() }
            .distinct()
    }

    override fun subscribe(topic: String, params: Any?, matcher: Matcher): Flux<ByteArray> {
        val up = multistream.getUpstreams()
            .filter { it.isAvailable() }
            .shuffled()
            .first { matcher.matches(it) } as GenericUpstream

        return up.getIngressSubscription().get<ByteArray>(topic, params)?.connect(matcher) ?: Flux.empty()
    }
}
