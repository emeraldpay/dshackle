/**
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

import org.reactivestreams.Subscriber
import reactor.core.publisher.EmitterProcessor
import reactor.core.publisher.Flux
import java.time.Duration
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToLong
import kotlin.random.Random

class FilteredApis<U : UpstreamApi>(
        allUpstreams: List<Upstream<U, *>>,
        private val matcher: Selector.Matcher,
        pos: Int,
        private val repeatLimit: Long,
        jitter: Int
) : ApiSource<U> {

    companion object {
        private const val DEFAULT_DELAY_STEP = 100
        private const val MAX_WAIT_MILLIS = 5000L
    }

    constructor(allUpstreams: List<Upstream<U, *>>,
                matcher: Selector.Matcher,
                pos: Int) : this(allUpstreams, matcher, pos, 10, 7)

    constructor(allUpstreams: List<Upstream<U, *>>,
                matcher: Selector.Matcher) : this(allUpstreams, matcher, 0, 10, 10)

    private val delay: Int
    private val upstreams: List<Upstream<UpstreamApi, *>>

    private val control = EmitterProcessor.create<Boolean>(32, false)

    init {
        delay = if (jitter > 0) {
            Random.nextInt(DEFAULT_DELAY_STEP - jitter, DEFAULT_DELAY_STEP + jitter)
        } else {
            DEFAULT_DELAY_STEP
        }

        upstreams = if (allUpstreams.size == 1 || pos == 0 || allUpstreams.isEmpty()) {
            allUpstreams
        } else {
            val safePosition = pos % allUpstreams.size
            allUpstreams.subList(safePosition, allUpstreams.size) + allUpstreams.subList(0, safePosition)
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

    override fun subscribe(subscriber: Subscriber<in U>) {
        val first = Flux.fromIterable(upstreams)
        val retries = (1 until repeatLimit).map { r ->
            Flux.fromIterable(upstreams).delaySubscription(waitDuration(r))
        }.let { Flux.concat(it) }

        Flux.concat(first, retries)
                .filter(Upstream<UpstreamApi, *>::isAvailable)
                .filter(matcher::matches)
                .flatMap { it.getApi(matcher) }
                .zipWith(control).map { it.t1 }
                .subscribe(subscriber as Subscriber<in UpstreamApi>)
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