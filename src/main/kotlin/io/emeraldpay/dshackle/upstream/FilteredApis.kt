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
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.reactivestreams.Subscriber
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class FilteredApis(
    val chain: Chain,
    private val allUpstreams: List<Upstream>,
    matcher: Selector.Matcher,
    private val pos: Int,
    private val retries: Int,
    sort: Selector.Sort = Selector.Sort.default,
) : ApiSource {
    private val internalMatcher: Selector.Matcher

    companion object {
        private val log = LoggerFactory.getLogger(FilteredApis::class.java)

        private const val DEFAULT_RETRY_LIMIT = 3

        private const val metricsCode = "select"

        @JvmStatic
        fun <T> startFrom(upstreams: List<T>, pos: Int): List<T> {
            return if (upstreams.size <= 1 || pos == 0) {
                upstreams
            } else {
                val safePosition = pos % upstreams.size
                upstreams.subList(safePosition, upstreams.size) + upstreams.subList(0, safePosition)
            }
        }

        private val metrics = EnumMap<Chain, Monitoring>(Chain::class.java)
        private val metricsSetup: Lock = ReentrantLock()
    }

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        matcher: Selector.Matcher,
        pos: Int,
    ) : this(chain, allUpstreams, matcher, pos, DEFAULT_RETRY_LIMIT)

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        upstreamFilter: Selector.UpstreamFilter,
        pos: Int,
    ) : this(chain, allUpstreams, upstreamFilter.matcher, pos, DEFAULT_RETRY_LIMIT, upstreamFilter.sort)

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        matcher: Selector.Matcher,
    ) : this(chain, allUpstreams, matcher, 0, DEFAULT_RETRY_LIMIT)

    private val primaryUpstreams: List<Upstream> = allUpstreams.filter {
        it.getRole() == UpstreamsConfig.UpstreamRole.PRIMARY
    }.let {
        startFrom(it, pos)
    }.sortedWith(sort.comparator)
    private val secondaryUpstreams: List<Upstream> = allUpstreams.filter {
        it.getRole() == UpstreamsConfig.UpstreamRole.SECONDARY
    }.let {
        startFrom(it, pos)
    }.sortedWith(sort.comparator)
    private val standardWithFallback: List<Upstream>

    private val counter: AtomicInteger = AtomicInteger(0)

    private var started = false
    private val control = Sinks.many().unicast().onBackpressureBuffer<Boolean>()
    private var upstreamsMatchesResponse: UpstreamsMatchesResponse? = UpstreamsMatchesResponse()

    init {
        val fallbackUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.FALLBACK
        }.let {
            startFrom(it, pos)
        }.sortedWith(sort.comparator)
        standardWithFallback = emptyList<Upstream>()
            .plus(primaryUpstreams)
            .plus(secondaryUpstreams)
            .plus(fallbackUpstreams)

        if (Global.metricsExtended) {
            getMetrics(chain).let { monitoring ->
                monitoring.countPrimary.record(primaryUpstreams.size.toDouble())
                monitoring.countSecondary.record(secondaryUpstreams.size.toDouble())
                monitoring.countFallback.record(fallbackUpstreams.size.toDouble())
            }
        }
        internalMatcher = Selector.MultiMatcher(
            listOf(Selector.AvailabilityMatcher(), matcher),
        )
    }

    private fun getMetrics(chain: Chain): Monitoring {
        val existing = metrics[chain]
        return if (existing == null) {
            metricsSetup.withLock {
                val existingDoubleCheck = metrics[chain]
                if (existingDoubleCheck != null) {
                    existingDoubleCheck
                } else {
                    val created = Monitoring(chain)
                    metrics[chain] = created
                    created
                }
            }
        } else {
            existing
        }
    }

    override fun subscribe(subscriber: Subscriber<in Upstream>) {
        // initially try only standard upstreams
        val first = Flux.fromIterable(primaryUpstreams.sortedBy { it.getStatus().grpcId })
        val second = Flux.fromIterable(secondaryUpstreams.sortedBy { it.getStatus().grpcId })
        // if all failed, try both standard and fallback upstreams, repeating in cycle
        val retries = (0 until this.retries).map {
            Flux.fromIterable(standardWithFallback.sortedBy { up -> up.getStatus().grpcId })
        }.let { Flux.concat(it) }

        val size = primaryUpstreams.size + secondaryUpstreams.size + standardWithFallback.size * this.retries
        var result = Flux.concat(first, second, retries).take(size.toLong(), false)

        if (Global.metricsExtended) {
            var count = 0
            result = result
                .doOnNext { count++ }
                .doFinally { metrics[chain]?.tried?.record(count.toDouble()) }
        }

        control.asFlux()
            .zipWith(result)
            .map { it.t2 }
            .filter { up ->
                val matchesResponse = internalMatcher.matchesWithCause(up)
                processMatchesResponse(up.getId(), matchesResponse)
                matchesResponse.matched()
                    .also {
                        if (!it) {
                            this.request(1)
                        }
                    }
            }
            .doOnNext {
                upstreamsMatchesResponse = null
                counter.incrementAndGet()
            }
            .doOnSubscribe {
                if (!started) {
                    // in addition to subscription the FilteredAPI should use request() method to prepare the control flow
                    log.warn("API Source subscribed before preparing a request")
                }
            }
            .subscribe(subscriber)
    }

    private fun processMatchesResponse(upstreamId: String, matchesResponse: MatchesResponse) {
        upstreamsMatchesResponse?.run {
            if (!matchesResponse.matched()) {
                addUpstreamMatchesResponse(upstreamId, matchesResponse)
            }
        }
    }

    override fun resolve() {
        control.tryEmitComplete()
    }

    override fun request(tries: Int) {
        started = true
        // TODO check the buffer size before submitting
        repeat(tries) {
            control.tryEmitNext(true)
        }
    }

    override fun attempts(): AtomicInteger =
        counter

    override fun upstreamsMatchesResponse(): UpstreamsMatchesResponse? = upstreamsMatchesResponse

    override fun toString(): String {
        return "Filter API: ${allUpstreams.size} upstreams with $internalMatcher"
    }

    class Monitoring(chain: Chain) {
        val countPrimary: DistributionSummary = DistributionSummary.builder("$metricsCode.exist")
            .description("Count of available upstreams to select")
            .tags(listOf(Tag.of("chain", chain.chainCode), Tag.of("role", "primary")))
            .register(Metrics.globalRegistry)
        val countSecondary: DistributionSummary = DistributionSummary.builder("$metricsCode.exist")
            .description("Count of available upstreams to select")
            .tags(listOf(Tag.of("chain", chain.chainCode), Tag.of("role", "secondary")))
            .register(Metrics.globalRegistry)
        val countFallback: DistributionSummary = DistributionSummary.builder("$metricsCode.exist")
            .description("Count of available fallback upstreams to select")
            .tags(listOf(Tag.of("chain", chain.chainCode), Tag.of("role", "fallback")))
            .register(Metrics.globalRegistry)
        val tried: DistributionSummary = DistributionSummary.builder("$metricsCode.tried")
            .description("How many upstreams were checked")
            .tags(listOf(Tag.of("chain", chain.chainCode)))
            .register(Metrics.globalRegistry)
    }
}
