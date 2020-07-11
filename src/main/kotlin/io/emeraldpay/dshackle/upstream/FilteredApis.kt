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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import org.reactivestreams.Subscriber
import reactor.core.publisher.EmitterProcessor
import reactor.core.publisher.Flux
import java.time.Duration
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToLong
import kotlin.random.Random

class FilteredApis(
        allUpstreams: List<Upstream>,
        private val matcher: Selector.Matcher,
        pos: Int,
        /**
         * Limit of retries
         */
        private val retryLimit: Long,
        jitter: Int
) : ApiSource {

    companion object {
        private const val DEFAULT_DELAY_STEP = 100
        private const val MAX_WAIT_MILLIS = 5000L

        @JvmStatic
        fun <T> startFrom(upstreams: List<T>, pos: Int): List<T> {
            return if (upstreams.size <= 1 || pos == 0) {
                upstreams
            } else {
                val safePosition = pos % upstreams.size
                upstreams.subList(safePosition, upstreams.size) + upstreams.subList(0, safePosition)
            }
        }
    }

    constructor(allUpstreams: List<Upstream>,
                matcher: Selector.Matcher,
                pos: Int) : this(allUpstreams, matcher, pos, 10, 7)

    constructor(allUpstreams: List<Upstream>,
                matcher: Selector.Matcher) : this(allUpstreams, matcher, 0, 10, 10)

    private val delay: Int
    private val standardUpstreams: List<Upstream>
    private val standardWithFallback: List<Upstream>

    private val control = EmitterProcessor.create<Boolean>(32, false)

    init {
        delay = if (jitter > 0) {
            Random.nextInt(DEFAULT_DELAY_STEP - jitter, DEFAULT_DELAY_STEP + jitter)
        } else {
            DEFAULT_DELAY_STEP
        }

        standardUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.STANDARD
        }.let {
            startFrom(it, pos)
        }
        val fallbackUpstreams = allUpstreams.filter {
            it.getRole() == UpstreamsConfig.UpstreamRole.FALLBACK
        }.let {
            startFrom(it, pos)
        }
        standardWithFallback = emptyList<Upstream>()
                .plus(standardUpstreams)
                .plus(fallbackUpstreams)

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
        val first = Flux.fromIterable(standardUpstreams)
        // if all failed, try both standard and fallback upstreams, repeating in cycle
        val retries = (0 until (retryLimit - 1)).map { r ->
            Flux.fromIterable(standardWithFallback)
                    // add delay to let upstream to restore if it's a temp failure
                    // but delay only start of the check, not between upstreams
                    // i.e. if all upstreams failed -> wait -> check all without waiting in between
                    .delaySubscription(waitDuration(r + 1))
        }.let { Flux.concat(it) }

        Flux.concat(first, retries)
                .filter(Upstream::isAvailable)
                .filter(matcher::matches)
                .zipWith(control)
                .map { it.t1 }
                .subscribe(subscriber)
    }

    override fun resolve() {
        control.onComplete()
    }

    override fun request(tries: Int) {
        //TODO check the buffer size before submitting
        repeat(tries) {
            control.onNext(true)
        }
    }
}