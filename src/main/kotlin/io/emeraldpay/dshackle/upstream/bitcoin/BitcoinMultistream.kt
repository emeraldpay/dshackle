/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.DistanceExtractor
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.EmptyEgressSubscription
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Selector.Matcher
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

@Suppress("UNCHECKED_CAST")
open class BitcoinMultistream(
    chain: Chain,
    private val sourceUpstreams: MutableList<BitcoinUpstream>,
    caches: Caches,
    private val headScheduler: Scheduler,
) : Multistream(chain, sourceUpstreams as MutableList<Upstream>, caches), Lifecycle {

    private var head: Head = EmptyHead()
    private var esplora = sourceUpstreams.find { it.esploraClient != null }?.esploraClient
    private var reader = BitcoinReader(this, head, esplora)
    private var addressActiveCheck: AddressActiveCheck? = null
    private var xpubAddresses: XpubAddresses? = null
    private var callRouter: LocalCallRouter = LocalCallRouter(DefaultBitcoinMethods(), reader)

    override fun init() {
        if (sourceUpstreams.size > 0) {
            head = updateHead()
        }
        super.init()
    }

    open val upstreams: List<BitcoinUpstream>
        get() {
            return sourceUpstreams
        }

    open fun getXpubAddresses(): XpubAddresses? {
        return xpubAddresses
    }

    fun updateHead(): Head {
        head.let {
            if (it is Lifecycle) {
                it.stop()
            }
        }
        lagObserver?.stop()
        lagObserver = null
        val head = if (sourceUpstreams.size == 1) {
            val upstream = sourceUpstreams.first()
            upstream.setLag(0)
            upstream.getHead().apply {
                if (this is Lifecycle) {
                    this.start()
                }
            }
        } else {
            val newHead = MergedHead(sourceUpstreams.map { it.getHead() }, MostWorkForkChoice(), headScheduler).apply { this.start() }
            newHead
        }
        onHeadUpdated(head)
        return head
    }

    /**
     * Finds an API that executed directly on a remote.
     */
    open fun getDirectApi(matcher: Selector.Matcher): Mono<JsonRpcReader> {
        val apis = getApiSource(matcher)
        apis.request(1)
        return Mono.from(apis)
            .map(Upstream::getIngressReader)
            .switchIfEmpty(Mono.error(Exception("No API available for $chain")))
    }

    override fun getLocalReader(localEnabled: Boolean): Mono<JsonRpcReader> {
        return Mono.just(callRouter)
    }

    open fun getReader(): BitcoinReader {
        return reader
    }

    override fun onUpstreamsUpdated() {
        super.onUpstreamsUpdated()
        esplora = sourceUpstreams.find { it.esploraClient != null }?.esploraClient
        reader = BitcoinReader(this, this.head, esplora)
        addressActiveCheck = esplora?.let { AddressActiveCheck(it) }
        xpubAddresses = addressActiveCheck?.let { XpubAddresses(it) }
        callRouter = LocalCallRouter(getMethods(), reader)
    }

    fun setHead(head: Head) {
        this.head = head
        reader = BitcoinReader(this, head, esplora)
    }

    override fun getHead(): Head {
        return head
    }

    override fun getEnrichedHead(mather: Matcher): Head {
        TODO("Not yet implemented")
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return sourceUpstreams.flatMap { it.getLabels() }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun getEgressSubscription(): EgressSubscription {
        return EmptyEgressSubscription()
    }

    override fun isRunning(): Boolean {
        return super.isRunning() || reader.isRunning()
    }

    override fun makeLagObserver(): HeadLagObserver {
        return HeadLagObserver(head, sourceUpstreams, DistanceExtractor::extractPowDistance, headScheduler, 3)
    }

    override fun getCachingReader(): CachingReader? {
        return null
    }

    override fun getHead(mather: Matcher): Head {
        return getHead()
    }

    override fun start() {
        super.start()
        reader.start()
    }

    override fun stop() {
        super.stop()
        reader.stop()
    }

    override fun addHead(upstream: Upstream) {
        setHead(updateHead())
    }

    override fun removeHead(upstreamId: String) {
        setHead(updateHead())
    }
}
