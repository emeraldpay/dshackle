/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.NoCallMethods
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Predicate
import kotlin.concurrent.withLock

/**
 * Aggregation of multiple upstreams responding to a single blockchain
 */
abstract class Multistream(
    val chain: Chain,
    private val upstreams: MutableList<Upstream>,
    val caches: Caches,
) : DshackleRpcReader, Lifecycle, HasEgressSubscription {

    companion object {
        private val log = LoggerFactory.getLogger(Multistream::class.java)
        private const val metrics = "upstreams"
    }

    private var cacheSubscription: Disposable? = null
    private val reconfigLock = ReentrantLock()
    protected val callMethods: AtomicReference<CallMethods> = AtomicReference(NoCallMethods())
    private var seq = 0
    protected var lagObserver: HeadLagObserver? = null
    private var subscription: Disposable? = null
    private var capabilities: Set<Capability> = emptySet()

    init {
        UpstreamAvailability.values().forEach { status ->
            Metrics.gauge(
                "$metrics.availability",
                listOf(Tag.of("chain", chain.chainCode), Tag.of("status", status.name.lowercase(Locale.getDefault()))),
                this
            ) {
                upstreams.count { it.getStatus() == status }.toDouble()
            }
        }

        Metrics.gauge(
            "$metrics.connected",
            listOf(Tag.of("chain", chain.chainCode)), this
        ) {
            upstreams.size.toDouble()
        }

        upstreams.forEach { up ->
            monitorUpstream(up)
            up.onUpdate(::onUpstreamsUpdated)
        }
    }

    private fun monitorUpstream(upstream: Upstream) {
        Metrics.gauge(
            "$metrics.lag",
            listOf(Tag.of("chain", chain.chainCode), Tag.of("upstream", upstream.getId())), upstream
        ) {
            it.getLag().toDouble()
        }
    }

    open fun init() {
        onUpstreamsUpdated()
    }

    /**
     * Get list of all underlying upstreams
     */
    open fun getAll(): List<Upstream> {
        return upstreams
    }

    /**
     * Add an upstream
     */
    fun addUpstream(upstream: Upstream) {
        upstreams.add(upstream)
        onUpstreamsUpdated()
        setHead(updateHead())
        monitorUpstream(upstream)
    }

    fun removeUpstream(id: String) {
        if (upstreams.removeIf { it.getId() == id }) {
            onUpstreamsUpdated()
            setHead(updateHead())
        }
    }

    /**
     * Get a source for direct APIs
     */
    open fun getApiSource(matcher: Selector.Matcher): ApiSource {
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return FilteredApis(chain, upstreams, matcher, i)
    }

    abstract fun getHead(): Head

    abstract fun getFeeEstimation(): ChainFees

    open fun onUpstreamsUpdated() {
        log.debug("Updating state of upstreams for $chain")
        upstreams.map { it.getMethods() }.let {
            if (it.size > 1) {
                AggregatedCallMethods(it)
            } else if (it.size == 1) {
                it.first()
            } else {
                NoCallMethods()
            }
        }.let(this.callMethods::set)
        capabilities = if (upstreams.isEmpty()) {
            emptySet()
        } else {
            upstreams.map(Upstream::getCapabilities)
                .reduce { acc, curr -> acc + curr }
        }
    }

    fun observeStatus(): Flux<UpstreamAvailability> {
        val upstreamsFluxes = getAll().map { up ->
            Flux.concat(
                Mono.just(up.getStatus()),
                up.observeStatus()
            ).map { UpstreamStatus(up, it) }
        }
        return Flux.merge(upstreamsFluxes)
            .filter(FilterBestAvailability())
            .map { it.status }
    }

    fun isAvailable(): Boolean {
        return getAll().any { it.isAvailable() }
    }

    fun getStatus(): UpstreamAvailability {
        val upstreams = getAll()
        return if (upstreams.isEmpty()) UpstreamAvailability.UNAVAILABLE
        else upstreams.minOf { it.getStatus() }
    }

    open fun getMethods(): CallMethods {
        return callMethods.get()
    }

    override fun start() {
        val repeated = Flux.interval(Duration.ofSeconds(30))
        val whenChanged = observeStatus()
            .distinctUntilChanged()
        subscription = Flux.merge(repeated, whenChanged)
            // print status _change_ every 15 seconds, at most; otherwise prints it on interval of 30 seconds
            .sample(Duration.ofSeconds(15))
            .subscribe { printStatus() }
    }

    override fun stop() {
        cacheSubscription?.dispose()
        cacheSubscription = null
        subscription?.dispose()
        subscription = null
        getHead().let {
            if (it is Lifecycle) {
                it.stop()
            }
        }
        lagObserver?.stop()
    }

    fun onHeadUpdated(head: Head) {
        reconfigLock.withLock {
            cacheSubscription?.dispose()
            cacheSubscription = head.getFlux().subscribe {
                caches.cache(Caches.Tag.LATEST, it)
            }
        }
        caches.setHead(head)
    }

    abstract fun updateHead(): Head
    abstract fun setHead(head: Head)

    override fun isRunning(): Boolean {
        return subscription != null
    }

    fun getBlockchain(): Chain {
        return chain
    }

    abstract fun <T : Multistream> cast(selfType: Class<T>): T

    fun printStatus() {
        var height: Long? = null
        try {
            height = getHead().getCurrentHeight()
        } catch (e: java.lang.IllegalStateException) {
            // timout
        } catch (e: Exception) {
            log.warn("Head processing error: ${e.javaClass} ${e.message}")
        }
        val statuses = upstreams.map { it.getStatus() }
            .groupBy { it }
            .map { "${it.key.name}/${it.value.size}" }
            .joinToString(",")
        val lag = upstreams
            .map {
                // by default, when no lag is available it uses Long.MAX_VALUE, and it doesn't make sense to print
                // status with such value. use NA (as Not Available) instead
                val value = it.getLag()
                if (value == Long.MAX_VALUE) {
                    "NA"
                } else {
                    value.toString()
                }
            }
            .joinToString(", ")
        val weak = upstreams
            .filter { it.getStatus() != UpstreamAvailability.OK }
            .joinToString(", ") { it.getId() }

        log.info("State of ${chain.chainCode}: height=${height ?: '?'}, status=[$statuses], lag=[$lag], weak=[$weak]")
    }

    // --------------------------------------------------------------------------------------------------------

    class UpstreamStatus(val upstream: Upstream, val status: UpstreamAvailability, val ts: Instant = Instant.now())

    class FilterBestAvailability : Predicate<UpstreamStatus> {
        private val lastRef = AtomicReference<UpstreamStatus>()

        override fun test(t: UpstreamStatus): Boolean {
            val curr = lastRef.updateAndGet { last ->
                val changed = last == null ||
                    t.status < last.status ||
                    (last.upstream == t.upstream && t.status != last.status) ||
                    last.ts.isBefore(t.ts - Duration.ofSeconds(60))
                if (changed) {
                    t
                } else {
                    last
                }
            }
            return curr == t
        }
    }
}
