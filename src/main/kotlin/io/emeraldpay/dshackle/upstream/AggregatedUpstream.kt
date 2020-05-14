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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.cache.*
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
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
abstract class AggregatedUpstream(
        val caches: Caches
) : Upstream, Lifecycle {

    private var cacheSubscription: Disposable? = null
    private val reconfigLock = ReentrantLock()
    private var callMethods: CallMethods? = null

    /**
     * Get list of all underlying upstreams
     */
    abstract fun getAll(): List<Upstream>

    /**
     * Add an upstream
     */
    abstract fun addUpstream(upstream: Upstream)

    /**
     * Get a source for direct APIs
     */
    abstract fun getApiSource(matcher: Selector.Matcher): ApiSource

    /**
     * Finds an API that executed directly on a remote.
     */
    abstract fun getDirectApi(matcher: Selector.Matcher): Mono<Reader<JsonRpcRequest, JsonRpcResponse>>

    /**
     * Finds an API that leverages caches and other optimizations/transformations of the request.
     */
    abstract fun getRoutedApi(matcher: Selector.Matcher): Mono<Reader<JsonRpcRequest, JsonRpcResponse>>

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        throw NotImplementedError("Immediate direct API is not implemented for Aggregated Upstream")
    }

    fun onUpstreamsUpdated() {
        reconfigLock.withLock {
            getAll().map { it.getMethods() }.let {
                callMethods = AggregatedCallMethods(it)
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

    override fun getOptions(): UpstreamsConfig.Options {
        return UpstreamsConfig.Options()
    }

    override fun getMethods(): CallMethods {
        return callMethods ?: throw IllegalStateException("Methods are not initialized yet")
    }

    override fun start() {
    }

    override fun stop() {
        cacheSubscription?.dispose()
        cacheSubscription = null
    }

    fun onHeadUpdated(head: Head) {
        reconfigLock.withLock {
            cacheSubscription?.dispose()
            cacheSubscription = head.getFlux().subscribe {
                caches.cache(Caches.Tag.LATEST, it)
            }
        }
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