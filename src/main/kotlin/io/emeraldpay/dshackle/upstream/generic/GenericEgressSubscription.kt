package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector.Matcher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler

class GenericEgressSubscription(
    val multistream: Multistream,
    val scheduler: Scheduler,
) : EgressSubscription {

    companion object {
        private val log = LoggerFactory.getLogger(GenericEgressSubscription::class.java)
    }

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

        val result = up.getIngressSubscription().get<ByteArray>(topic, params)?.connect(matcher)
        if (result == null) {
            log.warn("subscription source not found for topic {}", topic)
            return Flux.empty()
        }
        return up.getIngressSubscription().get<ByteArray>(topic, params)?.connect(matcher) ?: Flux.empty()
    }
}
