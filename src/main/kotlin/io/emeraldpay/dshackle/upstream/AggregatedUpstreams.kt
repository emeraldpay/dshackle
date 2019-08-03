package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import reactor.core.publisher.Flux
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Predicate

abstract class AggregatedUpstreams: Upstream {

    abstract fun getAll(): List<Upstream>
    abstract fun addUpstream(upstream: Upstream)
    abstract fun getApis(quorum: Int, matcher: Selector.Matcher): Iterator<EthereumApi>

    override fun observeStatus(): Flux<UpstreamAvailability> {
        val upstreamsFluxes = getAll().map { up -> up.observeStatus().map { UpstreamStatus(up, it) } }
        return Flux.merge(upstreamsFluxes)
                .filter(FilterBestAvailability())
                .map { it.status }
    }

    override fun getSupportedTargets(): Set<String> {
        val list = HashSet<String>()
        getAll().forEach {
            list.addAll(it.getSupportedTargets())
        }
        return list
    }

    override fun isAvailable(matcher: Selector.Matcher): Boolean {
        return getAll().any { it.isAvailable(matcher) }
    }

    override fun getStatus(): UpstreamAvailability {
        val upstreams = getAll()
        return if (upstreams.isEmpty()) UpstreamAvailability.UNAVAILABLE
        else upstreams.map { it.getStatus() }.min()!!
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return UpstreamsConfig.Options()
    }

    class UpstreamStatus(val upstream: Upstream, val status: UpstreamAvailability, val ts: Instant = Instant.now())

    class FilterBestAvailability(): Predicate<UpstreamStatus> {
        private val lastRef = AtomicReference<UpstreamStatus>()

        override fun test(t: UpstreamStatus): Boolean {
            val last = lastRef.get()
            val changed = last == null
                    || t.status > last.status
                    || (last.upstream == t.upstream && t.status != last.status)
                    || last.ts.isBefore(Instant.now() - Duration.ofSeconds(60))
            if (changed) {
                lastRef.set(t)
            }
            return changed
        }
    }
}