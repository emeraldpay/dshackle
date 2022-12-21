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
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.AggregatedPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.NoPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import org.springframework.util.ConcurrentReferenceHashMap
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Suppress("UNCHECKED_CAST")
open class EthereumMultistream(
    chain: Chain,
    val upstreams: MutableList<EthereumUpstream>,
    caches: Caches
) : Multistream(chain, upstreams as MutableList<Upstream>, caches), EthereumLikeMultistream {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumMultistream::class.java)
    }

    private var head: Head? = null
    private val filteredHeads: MutableMap<String, Head> =
        ConcurrentReferenceHashMap(16, ConcurrentReferenceHashMap.ReferenceType.WEAK)

    private val reader: EthereumCachingReader = EthereumCachingReader(this, this.caches, getMethodsFactory())
    private var subscribe = EthereumSubscriptionApi(this, NoPendingTxes())

    private val supportsEIP1559 = when (chain) {
        Chain.ETHEREUM, Chain.TESTNET_ROPSTEN, Chain.TESTNET_GOERLI, Chain.TESTNET_RINKEBY -> true
        else -> false
    }
    private val feeEstimation = if (supportsEIP1559) EthereumPriorityFees(this, reader, 256)
    else EthereumLegacyFees(this, reader, 256)

    init {
        this.init()
    }

    override fun init() {
        if (upstreams.size > 0) {
            head = updateHead()
        }
        super.init()
    }

    override fun onUpstreamsUpdated() {
        super.onUpstreamsUpdated()

        val pendingTxes: PendingTxesSource = upstreams
            .mapNotNull {
                it.getUpstreamSubscriptions().getPendingTxes()
            }.let {
                if (it.isEmpty()) {
                    NoPendingTxes()
                } else if (it.size == 1) {
                    it.first()
                } else {
                    AggregatedPendingTxes(it)
                }
            }
        subscribe = EthereumSubscriptionApi(this, pendingTxes)
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
            it.getBlockchainApi().nativeSubscribe(request)
        }?.let {
            Flux.merge(it)
        }

    override fun setHead(head: Head) {
        this.head = head
    }

    override fun updateHead(): Head {
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
            upstream.getHead().apply {
                if (this is Lifecycle) {
                    this.start()
                }
            }
        } else {
            val heads = upstreams.map { it.getHead() }
            val newHead = MergedHead(heads, MostWorkForkChoice(), "ETH Multistream").apply {
                this.start()
            }
            val lagObserver = EthereumHeadLagObserver(newHead, upstreams as Collection<Upstream>)
            this.lagObserver = lagObserver
            lagObserver.start()
            newHead
        }
        filteredHeads[Selector.AnyLabelMatcher().describeInternal()] = head
        onHeadUpdated(head)
        return head
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

    override fun getSubscriptionApi(): EthereumSubscriptionApi {
        return subscribe
    }

    override fun getHead(mather: Selector.Matcher): Head =
        filteredHeads.computeIfAbsent(mather.describeInternal().intern()) { _ ->
            upstreams.filter { mather.matches(it) }
                .apply {
                    log.debug("Found $size upstreams matching [${mather.describeInternal()}]")
                }.let {
                    val selected = it.map { it.getHead() }
                    when (it.size) {
                        0 -> EmptyHead()
                        1 -> selected.first()
                        else -> MergedHead(selected, MostWorkForkChoice(), "Eth head ${it.map { it.getId() }}").apply {
                            start()
                        }
                    }
                }
        }

    override fun getFeeEstimation(): ChainFees {
        return feeEstimation
    }
}
