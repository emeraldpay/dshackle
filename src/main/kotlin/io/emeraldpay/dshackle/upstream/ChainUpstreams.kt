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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Mono

/**
 * General interface to upstream(s) to a single chain
 */
abstract class ChainUpstreams<U : UpstreamApi, B>(
        val chain: Chain,
        private val upstreams: MutableList<Upstream<U, B>>,
        caches: Caches,
        objectMapper: ObjectMapper
) : AggregatedUpstream<U, B>(objectMapper, caches), Lifecycle {

    private val log = LoggerFactory.getLogger(ChainUpstreams::class.java)
    private var seq = 0
    protected var lagObserver: HeadLagObserver<U, B>? = null
    private var subscription: Disposable? = null

    open fun init() {
        onUpstreamsUpdated()
    }

    abstract fun updateHead(): Head<B>
    abstract fun setHead(head: Head<B>)

    override fun getId(): String {
        return "!all:${chain.chainCode}"
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        super.start()
        subscription = observeStatus()
                .distinctUntilChanged()
                .subscribe { printStatus() }
    }

    override fun stop() {
        super.stop()
        subscription?.dispose()
        subscription = null
        getHead().let {
            if (it is Lifecycle) {
                it.stop()
            }
        }
        lagObserver?.stop()
    }

    override fun getAll(): List<Upstream<U, B>> {
        return upstreams
    }

    override fun addUpstream(upstream: Upstream<U, B>) {
        upstreams.add(upstream)
        setHead(updateHead())
        onUpstreamsUpdated()
    }

    fun removeUpstream(id: String) {
        if (upstreams.removeIf { it.getId() == id }) {
            setHead(updateHead())
            onUpstreamsUpdated()
        }
    }

    override fun getApis(matcher: Selector.Matcher): ApiSource<U> {
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return FilteredApis(upstreams, matcher, i)
    }

    override fun getApi(matcher: Selector.Matcher): Mono<U> {
        val apis = getApis(matcher)
        apis.request(1)
        return Mono.from(apis)
                .switchIfEmpty(Mono.error<U>(Exception("No API available")))
    }

    override fun setLag(lag: Long) {
    }

    override fun getLag(): Long {
        return 0
    }

    abstract fun printStatus()
}