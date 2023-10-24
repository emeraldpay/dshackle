/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.DistanceExtractor
import io.emeraldpay.dshackle.upstream.DynamicMergedHead
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.EmptyEgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector.Matcher
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.forkchoice.PriorityForkChoice
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

@Suppress("UNCHECKED_CAST")
open class GenericMultistream(
    chain: Chain,
    val upstreams: MutableList<GenericUpstream>,
    caches: Caches,
    private val headScheduler: Scheduler,
) : Multistream(chain, upstreams as MutableList<Upstream>, caches) {

    private var head: DynamicMergedHead = DynamicMergedHead(
        PriorityForkChoice(),
        "Multistream of ${chain.chainCode}",
        headScheduler,
    )

    init {
        this.init()
    }

    override fun init() {
        if (upstreams.size > 0) {
            upstreams.forEach { addHead(it) }
        }
        super.init()
    }

    override fun start() {
        super.start()
        head.start()
        onHeadUpdated(head)
    }

    override fun addHead(upstream: Upstream) {
        val newHead = upstream.getHead()
        if (newHead is Lifecycle && !newHead.isRunning()) {
            newHead.start()
        }
        head.addHead(upstream)
    }

    override fun removeHead(upstreamId: String) {
        head.removeHead(upstreamId)
    }

    override fun makeLagObserver(): HeadLagObserver =
        HeadLagObserver(head, upstreams, DistanceExtractor::extractPriorityDistance, headScheduler, 6).apply {
            start()
        }

    override fun getCachingReader(): CachingReader? {
        return null
    }

    override fun getHead(mather: Matcher): Head {
        return getHead()
    }

    override fun getHead(): Head {
        return head
    }

    override fun getEnrichedHead(mather: Matcher): Head {
        return getHead()
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return upstreams.flatMap { it.getLabels() }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun getLocalReader(localEnabled: Boolean): Mono<JsonRpcReader> {
        return Mono.just(LocalReader(getMethods()))
    }

    override fun getEgressSubscription(): EgressSubscription {
        return EmptyEgressSubscription()
    }
}
