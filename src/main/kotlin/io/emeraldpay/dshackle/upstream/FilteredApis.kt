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
import java.time.Duration
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToLong
import kotlin.random.Random

class FilteredApis(
    val chain: Chain,
    private val allUpstreams: List<Upstream>,
    private val matcher: Selector.Matcher,
    private val pos: Int,
    /**
     * Limit of retries
     */
    private val retryLimit: Long,
    jitter: Int
) : ApiSource {

    companion object {
        private val log = LoggerFactory.getLogger(FilteredApis::class.java)

        private const val DEFAULT_DELAY_STEP = 100
        private const val MAX_WAIT_MILLIS = 5000L

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
        pos: Int
    ) : this(chain, allUpstreams, matcher, pos, 10, 7)

    constructor(
        chain: Chain,
        allUpstreams: List<Upstream>,
        matcher: Selector.Matcher
    ) : this(chain, allUpstreams, matcher, 0, 10, 10)

    private val delay: Int
    private val primaryUpstreams: List<Upstream>
    private val secondaryUpstreams: List<Upstream>
    private val standardWithFallback: List<Upstream>

    private val counter: AtomicInteger = AtomicInteger(0)

    private var started = false
    private val control = Sinks.many().unicast().onBackpressureBuffer<Boolean>()

    init {
        delay = if (jitter > 0) {
            Random.nextInt(DEFAULT_DELAY_STEP - jitter, DEFAULT_DELAY_STEP + jitter)
        } else {
            DEFAULT_DELAY_STEP
        }

        primaryUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.PRIMARY
        }.let {
            startFrom(it, pos)
        }
        secondaryUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.SECONDARY
        }.let {
            startFrom(it, pos)
        }
        val fallbackUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.FALLBACK
        }.let {
            startFrom(it, pos)
        }
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

    fun waitDuration(rawn: Long): Duration {
        val n = max(rawn, 1)
        val time = min(
            (n.toDouble().pow(2.0) * delay).roundToLong(),
            MAX_WAIT_MILLIS
        )
        return Duration.ofMillis(time)
    }

    override fun subscribe(subscriber: Subscriber<in Upstream>) {
        // initially try only standard upstreams
        val first = Flux.fromIterable(primaryUpstreams)
        val second = Flux.fromIterable(secondaryUpstreams)
        // if all failed, try both standard and fallback upstreams, repeating in cycle
        val retries = (0 until (retryLimit - 1)).map { r ->
            Flux.fromIterable(standardWithFallback)
                // add a delay to let upstream to restore if it's a temp failure
                // but delay only start of the check, not between upstreams
                // i.e. if all upstreams failed -> wait -> check all without waiting in between
                .delaySubscription(waitDuration(r + 1))
        }.let { Flux.concat(it) }

        var result = Flux.concat(first, second, retries)

        if (Global.metricsExtended) {
            var count = 0
            result = result
                .doOnNext { count++ }
                .doFinally { metrics[chain]?.tried?.record(count.toDouble()) }
        }

        result.filter { up ->
            (up.isAvailable() && matcher.matches(up)).also {
                if (it) {
                    counter.incrementAndGet()
                }
            }
        }
            .zipWith(control.asFlux())
            .map { it.t1 }
            .doOnSubscribe {
                if (!started) {
                    // in addition to subscription the FilteredAPI should use request() method to prepare the control flow
                    log.warn("API Source subscribed before preparing a request")
                }
            }
            .subscribe(subscriber)
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

    override fun toString(): String {
        return "Filter API: ${allUpstreams.size} upstreams with $matcher"
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
