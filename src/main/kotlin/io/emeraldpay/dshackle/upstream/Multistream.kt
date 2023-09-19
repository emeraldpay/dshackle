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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.apache.commons.collections4.Factory
import org.apache.commons.collections4.FunctorException
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Aggregation of multiple upstreams responding to a single blockchain
 */
abstract class Multistream(
    val chain: Chain,
    private val upstreams: MutableList<Upstream>,
    val caches: Caches,
) : Upstream, Lifecycle, HasEgressSubscription {

    companion object {
        private const val metrics = "upstreams"
    }

    protected val log = LoggerFactory.getLogger(this::class.java)

    private var started = false

    private var cacheSubscription: Disposable? = null
    private val reconfigLock = ReentrantLock()
    private val eventLock = ReentrantLock()
    @Volatile
    private var callMethods: CallMethods? = null
    private var callMethodsFactory: Factory<CallMethods> = Factory {
        return@Factory callMethods ?: throw FunctorException("Not initialized yet")
    }
    private var seq = 0
    protected var lagObserver: HeadLagObserver? = null
    private var subscription: Disposable? = null
    @Volatile
    private var capabilities: Set<Capability> = emptySet()
    @Volatile
    private var quorumLabels: List<QuorumForLabels.QuorumItem>? = null
    private val removed: MutableMap<String, Upstream> = HashMap()
    private val meters: MutableMap<String, List<Meter.Id>> = HashMap()
    private val addedUpstreams = Sinks.many()
        .multicast()
        .directBestEffort<Upstream>()
    private val removedUpstreams = Sinks.many()
        .multicast()
        .directBestEffort<Upstream>()
    private val updateUpstreams = Sinks.many()
        .multicast()
        .directBestEffort<Upstream>()

    init {
        UpstreamAvailability.values().forEach { status ->
            Metrics.gauge(
                "$metrics.availability",
                listOf(Tag.of("chain", chain.chainCode), Tag.of("status", status.name.lowercase())),
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
        }
    }

    override fun getSubscriptionTopics(): List<String> {
        return getEgressSubscription().getAvailableTopics()
    }

    private fun removeUpstreamMeters(upstreamId: String) {
        meters[upstreamId]?.forEach {
            Metrics.globalRegistry.remove(it)
        }
    }

    private fun monitorUpstream(upstream: Upstream) {
        val upstreamId = upstream.getId()

        // otherwise metric will stuck with prev upstream instance
        removeUpstreamMeters(upstreamId)

        meters[upstreamId] = listOf(
            Gauge.builder("$metrics.lag", upstream) {
                it.getLag()?.toDouble() ?: Double.NaN
            }
                .tag("chain", chain.chainCode)
                .tag("upstream", upstreamId)
                .register(Metrics.globalRegistry)
                .id,
            Gauge.builder("$metrics.availability.status", upstream) { it.getStatus().grpcId.toDouble() }
                .tag("chain", chain.chainCode)
                .tag("upstream", upstreamId)
                .register(Metrics.globalRegistry)
                .id,
        )
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
    fun addUpstream(upstream: Upstream): Boolean =
        upstreams.none {
            it.getId() == upstream.getId()
        }.also {
            if (it) {
                upstreams.add(upstream)
                removed.remove(upstream.getId())
                addHead(upstream)
                monitorUpstream(upstream)
            }
        }

    fun removeUpstream(id: String): Boolean =
        upstreams.removeIf { up ->
            (up.getId() == id).also {
                if (it) {
                    removed[id] = up
                }
            }
        }.also {
            if (it) {
                removeHead(id)
                removeUpstreamMeters(id)
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

    abstract fun getFeeEstimation(): ChainFees

    /**
     * Finds an API that leverages caches and other optimizations/transformations of the request.
     */
    abstract fun getLocalReader(localEnabled: Boolean): Mono<JsonRpcReader>

    override fun getIngressReader(): JsonRpcReader {
        throw NotImplementedError("Immediate direct API is not implemented for Aggregated Upstream")
    }

    open fun onUpstreamsUpdated() {
        reconfigLock.withLock {
            val upstreams = getAll()
            val availableUpstreams = upstreams.filter { it.isAvailable() }
            availableUpstreams.map { it.getMethods() }.let {
                callMethods = AggregatedCallMethods(it)
            }
            capabilities = if (upstreams.isEmpty()) {
                emptySet()
            } else {
                availableUpstreams.map { up ->
                    up.getCapabilities()
                }.let {
                    if (it.isNotEmpty()) {
                        it.reduce { acc, curr -> acc + curr }
                    } else {
                        emptySet()
                    }
                }
            }
            quorumLabels = getQuorumLabels(availableUpstreams)
            when {
                upstreams.size == 1 -> {
                    lagObserver?.stop()
                    lagObserver = null
                    upstreams[0].setLag(0)
                }
                upstreams.size > 1 -> if (lagObserver == null) lagObserver = makeLagObserver()
            }
        }
    }

    private fun getQuorumLabels(ups: List<Upstream>): List<QuorumForLabels.QuorumItem> {
        val nodes = QuorumForLabels()
        ups.forEach { up ->
            if (up is DefaultUpstream) {
                nodes.add(up.getQuorumByLabel())
            }
        }
        return nodes.getAll()
    }

    fun getQuorumLabels(): List<QuorumForLabels.QuorumItem> = quorumLabels ?: emptyList()

    override fun observeStatus(): Flux<UpstreamAvailability> {
        val upstreamsFluxes = getAll().map { up ->
            Flux.concat(
                Mono.just(up.getStatus()),
                up.observeStatus()
            ).map { UpstreamStatus(up, it) }
        }
        return Flux.merge(upstreamsFluxes)
            .map(FilterBestAvailability())
            .distinct()
    }

    override fun isAvailable(): Boolean {
        return getAll().any { it.isAvailable() }
    }

    override fun getStatus(): UpstreamAvailability {
        val upstreams = getAll()
        return if (upstreams.isEmpty()) UpstreamAvailability.UNAVAILABLE
        else upstreams.minOf { it.getStatus() }
    }

    // TODO options for multistream are useless
    override fun getOptions(): UpstreamsConfig.Options {
        throw IllegalStateException("Options are not supported for multistream")
    }

    // TODO roles for multistream are useless
    override fun getRole(): UpstreamsConfig.UpstreamRole {
        return UpstreamsConfig.UpstreamRole.PRIMARY
    }

    override fun getMethods(): CallMethods {
        return callMethods ?: throw IllegalStateException("Methods are not initialized yet")
    }

    fun getMethodsFactory(): Factory<CallMethods> {
        return callMethodsFactory
    }

    override fun start() {
        val repeated = Flux.interval(Duration.ofSeconds(30))
        val whenChanged = observeStatus()
            .distinctUntilChanged()
        subscription = Flux.merge(repeated, whenChanged)
            // print status _change_ every 15 seconds, at most; otherwise prints it on interval of 30 seconds
            .sample(Duration.ofSeconds(15))
            .subscribe { printStatus() }

        observeUpstreamsStatuses()

        started = true
    }

    private fun observeUpstreamsStatuses() {
        subscribeAddedUpstreams()
            .distinctUntilChanged {
                it.getId()
            }.flatMap { upstream ->
                upstream.observeStatus().map { upstream }
                    .takeUntilOther(
                        subscribeRemovedUpstreams()
                            .filter { it.getId() == upstream.getId() }
                    )
            }
            .subscribe {
                onUpstreamChange(UpstreamChangeEvent(this.chain, it, UpstreamChangeEvent.ChangeType.UPDATED))
            }
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
        started = false
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

    abstract fun addHead(upstream: Upstream)
    abstract fun removeHead(upstreamId: String)

    override fun getId(): String {
        return "!all:${chain.chainCode}"
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun setLag(lag: Long) {
    }

    override fun getLag(): Long {
        return 0
    }

    override fun getCapabilities(): Set<Capability> {
        return this.capabilities
    }

    override fun isGrpc(): Boolean {
        return false
    }

    override fun nodeId(): Byte = 0

    fun printStatus() {
        var height: Long? = null
        try {
            height = getHead().getCurrentHeight()
        } catch (e: java.lang.IllegalStateException) {
            // timout
        } catch (e: Exception) {
            log.warn("Head processing error: ${e.javaClass} ${e.message}")
        }
        val statuses = upstreams.asSequence().plus(removed.values).map { it.getStatus() }
            .groupBy { it }
            .map { "${it.key.name}/${it.value.size}" }
            .joinToString(",")
        val lag = upstreams.plus(removed.values).joinToString(", ") {
            // by default, when no lag is available it uses Long.MAX_VALUE, and it doesn't make sense to print
            // status with such value. use NA (as Not Available) instead
            val value = it.getLag()
            value?.toString() ?: "NA"
        }
        val weak = upstreams.plus(removed.values)
            .filter { it.getStatus() != UpstreamAvailability.OK }
            .joinToString(", ") { it.getId() }

        val instance = System.identityHashCode(this).toString(16)
        log.info("State of ${chain.chainCode}: height=${height ?: '?'}, status=[$statuses], lag=[$lag], weak=[$weak] ($instance)")
    }

    fun test(event: UpstreamChangeEvent): Boolean {
        return event.chain == this.chain
    }

    @EventListener
    @Order(Ordered.HIGHEST_PRECEDENCE)
    fun onUpstreamChange(event: UpstreamChangeEvent) {
        val chain = event.chain
        if (this.chain == chain) {
            eventLock.withLock {
                log.debug("Processing event $event")
                when (event.type) {
                    UpstreamChangeEvent.ChangeType.REVALIDATED -> {}
                    UpstreamChangeEvent.ChangeType.UPDATED -> {
                        onUpstreamsUpdated()
                        updateUpstreams.emitNext(event.upstream) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                    }
                    UpstreamChangeEvent.ChangeType.ADDED -> {
                        if (!started) {
                            start()
                        }
                        if (event.upstream is CachesEnabled) {
                            event.upstream.setCaches(caches)
                        }
                        addUpstream(event.upstream).takeIf { it }?.let {
                            try {
                                addedUpstreams.emitNext(event.upstream) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                                onUpstreamsUpdated()
                                log.info("Upstream ${event.upstream.getId()} with chain $chain has been added")
                            } catch (e: Sinks.EmissionException) {
                                log.error("error during event processing $event", e)
                            }
                        }
                    }
                    UpstreamChangeEvent.ChangeType.REMOVED -> {
                        removeUpstream(event.upstream.getId()).takeIf { it }?.let {
                            try {
                                removedUpstreams.emitNext(event.upstream) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                                onUpstreamsUpdated()
                                log.info("Upstream ${event.upstream.getId()} with chain $chain has been removed")
                            } catch (e: Sinks.EmissionException) {
                                log.error("error during event processing $event", e)
                            }
                        }
                    }
                }
            }
        }
    }

    fun haveUpstreams(): Boolean =
        upstreams.isNotEmpty()

    fun hasMatchingUpstream(matcher: Selector.LabelSelectorMatcher): Boolean {
        return upstreams.any { matcher.matches(it) }
    }

    fun subscribeAddedUpstreams(): Flux<Upstream> =
        addedUpstreams.asFlux()

    fun subscribeRemovedUpstreams(): Flux<Upstream> =
        removedUpstreams.asFlux()
    fun subscribeUpdatedUpstreams(): Flux<Upstream> =
        updateUpstreams.asFlux()

    abstract fun makeLagObserver(): HeadLagObserver

    // --------------------------------------------------------------------------------------------------------

    class UpstreamStatus(val upstream: Upstream, val status: UpstreamAvailability)

    class FilterBestAvailability : java.util.function.Function<UpstreamStatus, UpstreamAvailability> {
        val map = ConcurrentHashMap<String, UpstreamAvailability>()
        override fun apply(t: UpstreamStatus): UpstreamAvailability {
            map[t.upstream.getId()] = t.status
            return map.values.min()
        }
    }
}
