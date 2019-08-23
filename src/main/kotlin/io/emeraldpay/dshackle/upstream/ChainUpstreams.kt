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
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import java.lang.IllegalStateException
import java.time.Duration

open class ChainUpstreams (
        val chain: Chain,
        private val upstreams: MutableList<Upstream>,
        targets: CallMethods,
        objectMapper: ObjectMapper
) : AggregatedUpstream(targets, objectMapper), Lifecycle {

    private val log = LoggerFactory.getLogger(ChainUpstreams::class.java)
    private var seq = 0
    private var head: EthereumHead?
    private var lagObserver: HeadLagObserver? = null
    private var subscription: Disposable? = null

    init {
        head = updateHead()
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
        head?.let {
            if (it is Lifecycle) {
                it.stop()
            }
        }
        lagObserver?.stop()
    }

    internal fun updateHead(): EthereumHead {
        head?.let {
            if (it is Lifecycle) {
                it.stop()
            }
        }
        lagObserver?.stop()
        lagObserver = null
        val head = if (upstreams.size == 1) {
            val upstream = upstreams.first()
            upstream.setLag(0)
            upstream.getHead()
        } else {
            val newHead = EthereumHeadMerge(upstreams.map { it.getHead().getFlux() }).apply {
                this.start()
            }
            val lagObserver = HeadLagObserver(newHead, upstreams).apply {
                this.start()
            }
            this.lagObserver = lagObserver
            newHead
        }
        onHeadUpdated(head)
        return head
    }

    override fun getAll(): List<Upstream> {
        return upstreams
    }

    override fun addUpstream(upstream: Upstream) {
        upstreams.add(upstream)
        head = updateHead()
    }

    override fun getApis(matcher: Selector.Matcher): Iterator<DirectEthereumApi> {
        val i = seq++
        if (seq >= Int.MAX_VALUE / 2) {
            seq = 0
        }
        return FilteringApiIterator(upstreams, i, matcher)
    }

    override fun getApi(matcher: Selector.Matcher): DirectEthereumApi {
        return getApis(matcher).next()
    }

    override fun getHead(): EthereumHead {
        return head!!
    }

    override fun setLag(lag: Long) {
    }

    override fun getLag(): Long {
        return 0
    }

    fun printStatus() {
        var height: Long? = null
        try {
            height = head!!.getFlux().next().block(Duration.ofSeconds(1))?.number
        } catch (e: IllegalStateException) {
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


}