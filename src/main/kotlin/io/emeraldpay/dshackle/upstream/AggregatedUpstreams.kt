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
    abstract fun getApis(quorum: Int): Iterator<EthereumApi>

    override fun observeStatus(): Flux<UpstreamAvailability> {
        val upstreamsFluxes = getAll().map { up -> up.observeStatus().map { UpstreamStatus(up, it) } }
        return Flux.merge(upstreamsFluxes)
                .filter(FilterBestAvailability())
                .map { it.status }
    }

    override fun isAvailable(): Boolean {
        return getAll().any { it.isAvailable() }
    }

    override fun getStatus(): UpstreamAvailability {
        val upstreams = getAll()
        return if (upstreams.isEmpty()) UpstreamAvailability.UNAVAILABLE
        else upstreams.map { it.getStatus() }.min()!!
    }

    override fun getOptions(): UpstreamsConfig.Options {
        val options = UpstreamsConfig.Options()
        options.quorum = getAll().filter {
            it.getStatus() == UpstreamAvailability.OK
        }.sumBy {
            it.getOptions().quorum
        }
        return options
    }

    class SingleApi(
            private val quorumApi: QuorumApi
    ): Iterator<EthereumApi> {

        private var consumed = false

        override fun hasNext(): Boolean {
            return !consumed && quorumApi.hasNext()
        }

        override fun next(): EthereumApi {
            consumed = true
            return quorumApi.next()
        }
    }

    class QuorumApi(
            private val apis: List<Upstream>,
            private val quorum: Int,
            private var pos: Int
    ): Iterator<EthereumApi> {

        private var consumed = 0

        override fun hasNext(): Boolean {
            return consumed < quorum
        }

        override fun next(): EthereumApi {
            val start = pos
            while (pos < start + apis.size) {
                val api = apis[pos++ % apis.size]
                if (api.isAvailable()) {
                    consumed++
                    return api.getApi()
                }
            }
            throw IllegalStateException("No upstream API available")
        }
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