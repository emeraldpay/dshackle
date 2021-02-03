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

import io.emeraldpay.dshackle.cache.*
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.apache.commons.collections4.Factory
import org.apache.commons.collections4.FunctorException
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
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
        val postprocessor: RequestPostprocessor
) : Upstream, Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(Multistream::class.java)
        private const val metrics = "upstreams"
    }

    private var cacheSubscription: Disposable? = null
    private val reconfigLock = ReentrantLock()
    private var callMethods: CallMethods? = null
    private var callMethodsFactory: Factory<CallMethods> = Factory {
        return@Factory callMethods ?: throw FunctorException("Not initialized yet")
    }
    private var seq = 0
    protected var lagObserver: HeadLagObserver? = null
    private var subscription: Disposable? = null
    private var capabilities: Set<Capability> = emptySet()

    init {
        UpstreamAvailability.values().forEach { status ->
            Metrics.gauge("$metrics.availability",
                    listOf(Tag.of("chain", chain.chainCode), Tag.of("status", status.name.toLowerCase()))
                    , this) {
                upstreams.count { it.getStatus() == status }.toDouble()
            }
        }

        upstreams.forEach { up ->
            Metrics.gauge("$metrics.lag",
                    listOf(Tag.of("chain", chain.chainCode), Tag.of("upstream", up.getId()))
                    , this) {
                up.getLag().toDouble()
            }
        }
    }

    open fun init() {
        onUpstreamsUpdated()
    }

    /**
     * Get list of all underlying upstreams
     */
    fun getAll(): List<Upstream> {
        return upstreams
    }

    /**
     * Add an upstream
     */
    fun addUpstream(upstream: Upstream) {
        upstreams.add(upstream)
        onUpstreamsUpdated()
        setHead(updateHead())
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
        return FilteredApis(upstreams, matcher, i)
    }

    /**
     * Finds an API that executed directly on a remote.
     */
    open fun getDirectApi(matcher: Selector.Matcher): Mono<Reader<JsonRpcRequest, JsonRpcResponse>> {
        val apis = getApiSource(matcher)
        apis.request(1)
        return Mono.from(apis)
                .map(Upstream::getApi)
                .switchIfEmpty(Mono.error(Exception("No API available for $chain")))
    }

    /**
     * Finds an API that leverages caches and other optimizations/transformations of the request.
     */
    abstract fun getRoutedApi(matcher: Selector.Matcher): Mono<Reader<JsonRpcRequest, JsonRpcResponse>>

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        throw NotImplementedError("Immediate direct API is not implemented for Aggregated Upstream")
    }

    open fun onUpstreamsUpdated() {
        reconfigLock.withLock {
            val upstreams = getAll()
            upstreams.map { it.getMethods() }.let {
                //TODO made list of uniq instances, and then if only one, just use it directly
                callMethods = AggregatedCallMethods(it)
            }
            capabilities = if (upstreams.isEmpty()) {
                emptySet()
            } else {
                upstreams.map { up ->
                    up.getCapabilities()
                }.reduce { acc, curr -> acc + curr }
            }
        }
    }

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

    //TODO options for multistream are useless
    override fun getOptions(): UpstreamsConfig.Options {
        return UpstreamsConfig.Options()
    }

    //TODO roles for multistream are useless
    override fun getRole(): UpstreamsConfig.UpstreamRole {
        return UpstreamsConfig.UpstreamRole.STANDARD
    }

    override fun getMethods(): CallMethods {
        return callMethods ?: throw IllegalStateException("Methods are not initialized yet")
    }

    fun getMethodsFactory(): Factory<CallMethods> {
        return callMethodsFactory
    }

    override fun start() {
        subscription = observeStatus()
                .distinctUntilChanged()
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

    fun printStatus() {
        var height: Long? = null
        try {
            height = getHead().getFlux().next().block(Duration.ofSeconds(1))?.height
        } catch (e: java.lang.IllegalStateException) {
            //timout
        } catch (e: Exception) {
            log.warn("Head processing error: ${e.javaClass} ${e.message}")
        }
        val statuses = upstreams.map { it.getStatus() }
                .groupBy { it }
                .map { "${it.key.name}/${it.value.size}" }
                .joinToString(",")
        val lag = upstreams.map { it.getLag() }
                .joinToString(", ")

        log.info("State of ${chain.chainCode}: height=${height ?: '?'}, status=$statuses, lag=[$lag]")
    }

    // --------------------------------------------------------------------------------------------------------

    class UpstreamStatus(val upstream: Upstream, val status: UpstreamAvailability, val ts: Instant = Instant.now())

    class FilterBestAvailability() : Predicate<UpstreamStatus> {
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