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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.forkchoice.PriorityForkChoice
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import org.springframework.util.ConcurrentReferenceHashMap
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Suppress("UNCHECKED_CAST")
open class EthereumPosMultiStream(
    chain: Chain,
    val upstreams: MutableList<EthereumPosUpstream>,
    caches: Caches
) : Multistream(chain, upstreams as MutableList<Upstream>, caches), EthereumLikeMultistream {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumPosMultiStream::class.java)
    }

    private var head: Head? = null

    private val reader: EthereumCachingReader = EthereumCachingReader(this, this.caches, getMethodsFactory())
    private val feeEstimation = EthereumPriorityFees(this, reader, 256)
    private val subscribe = EthereumSubscribe(this)
    private val filteredHeads: MutableMap<String, Head> =
        ConcurrentReferenceHashMap(16, ConcurrentReferenceHashMap.ReferenceType.WEAK)

    init {
        this.init()
    }

    override fun init() {
        if (upstreams.size > 0) {
            head = updateHead()
        }
        super.init()
    }

    override fun start() {
        super.start()
        reader.start()
    }

    override fun stop() {
        super.stop()
        reader.stop()
        filteredHeads.clear()
    }

    override fun isRunning(): Boolean {
        return super.isRunning() || reader.isRunning()
    }

    override fun getReader(): EthereumCachingReader {
        return reader
    }

    override fun getHead(): Head {
        return head!!
    }

    override fun tryProxy(
        matcher: Selector.Matcher,
        request: BlockchainOuterClass.NativeSubscribeRequest
    ): Flux<out Any>? =
        upstreams.filter {
            matcher.matches(it)
        }.takeIf { ups ->
            ups.size == 1 && ups.all { it.isGrpc() }
        }?.map {
            it as GrpcUpstream
        }?.map {
            it.proxySubscribe(request)
        }?.let {
            Flux.merge(it)
        }

    override fun setHead(head: Head) {
        this.head = head
    }

    override fun updateHead(): Head {
        this.head?.takeIf { it is Lifecycle }?.apply { stop() }
        lagObserver?.stop()
        lagObserver = null

        return when (upstreams.size) {
            0 -> EmptyHead()
            1 -> upstreams.first().let {
                it.setLag(0)
                it.getHead().apply {
                    if (this is Lifecycle) {
                        start()
                    }
                }
            }

            else -> upstreams.map { it.getHead() }.let { heads ->
                MergedHead(heads, PriorityForkChoice(), "ETH Pos Multistream").apply {
                    start()
                }.also {
                    this.lagObserver = EthereumPosHeadLagObserver(it, ArrayList(upstreams)).apply {
                        start()
                    }
                }
            }
        }.also {
            onHeadUpdated(it)
        }
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

    override fun getRoutedApi(localEnabled: Boolean): Mono<Reader<JsonRpcRequest, JsonRpcResponse>> {
        return Mono.just(LocalCallRouter(reader, getMethods(), getHead(), localEnabled))
    }

    override fun getSubscribe(): EthereumSubscribe {
        return subscribe
    }

    override fun getHead(mather: Selector.Matcher): Head =
        filteredHeads.computeIfAbsent(mather.describeInternal().intern()) { _ ->
            upstreams.filter { mather.matches(it) }
                .apply {
                    log.debug("Found $size upstreams matching [${mather.describeInternal()}]")
                }
                .let {
                    val selected = it.map { it.getHead() }
                    when (it.size) {
                        0 -> EmptyHead()
                        1 -> selected.first()
                        else -> MergedHead(selected, PriorityForkChoice(), "ETH head for ${it.map { it.getId() }}").apply {
                            start()
                        }
                    }
                }
        }

    override fun getFeeEstimation(): ChainFees {
        return feeEstimation
    }
}
