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

import io.grpc.Status
import io.grpc.StatusException
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.IndexConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.DistanceExtractor
import io.emeraldpay.dshackle.upstream.DynamicMergedHead
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Oracle
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.AggregatedPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.NoPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.forkchoice.PriorityForkChoice
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstream
import io.emeraldpay.etherjar.domain.BlockHash
import org.springframework.cloud.sleuth.Tracer
import org.springframework.util.ConcurrentReferenceHashMap
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

@Suppress("UNCHECKED_CAST")
open class EthereumMultistream(
    chain: Chain,
    val upstreams: MutableList<EthereumLikeUpstream>,
    caches: Caches,
    private val headScheduler: Scheduler,
    tracer: Tracer,
    estimateLogsCountConfig: IndexConfig.Index? = null,
) : Multistream(chain, upstreams as MutableList<Upstream>, caches), EthereumLikeMultistream {

    private var head: DynamicMergedHead = DynamicMergedHead(
        PriorityForkChoice(),
        "ETH Multistream of ${chain.chainCode}",
        headScheduler,
    )

    private val filteredHeads: MutableMap<String, Head> =
        ConcurrentReferenceHashMap(16, ConcurrentReferenceHashMap.ReferenceType.WEAK)

    private val reader: EthereumCachingReader = EthereumCachingReader(this, this.caches, getMethodsFactory(), tracer)
    private var subscribe = EthereumEgressSubscription(this, headScheduler, NoPendingTxes())

    private val supportsEIP1559set = setOf(
        Chain.ETHEREUM__MAINNET,
        Chain.ETHEREUM__GOERLI,
        Chain.ETHEREUM__SEPOLIA,
        Chain.ARBITRUM__MAINNET,
        Chain.OPTIMISM__MAINNET,
        Chain.ARBITRUM__GOERLI,
        Chain.OPTIMISM__GOERLI,
        Chain.POLYGON_ZKEVM__MAINNET,
        Chain.POLYGON_ZKEVM__TESTNET,
        Chain.ZKSYNC__MAINNET,
        Chain.ZKSYNC__TESTNET,
        Chain.ARBITRUM_NOVA__MAINNET,
    )

    private val supportsEIP1559 = supportsEIP1559set.contains(chain)

    private val feeEstimation = if (supportsEIP1559) {
        EthereumPriorityFees(this, reader, 256)
    } else {
        EthereumLegacyFees(this, reader, 256)
    }

    private val logsEstimation: Oracle? = estimateLogsCountConfig?.let {
        Oracle(estimateLogsCountConfig.store, estimateLogsCountConfig.limit ?: 0L)
    }

    init {
        this.init()
    }

    override fun init() {
        if (upstreams.size > 0) {
            upstreams.forEach { addHead(it) }
        }
        super.init()
    }

    override fun onUpstreamsUpdated() {
        super.onUpstreamsUpdated()

        val pendingTxes: PendingTxesSource = upstreams
            .mapNotNull {
                it.getIngressSubscription().getPendingTxes()
            }.let {
                if (it.isEmpty()) {
                    NoPendingTxes()
                } else if (it.size == 1) {
                    it.first()
                } else {
                    AggregatedPendingTxes(it)
                }
            }
        subscribe = EthereumEgressSubscription(this, headScheduler, pendingTxes)
    }

    override fun start() {
        super.start()
        head.start()
        onHeadUpdated(head)
        reader.start()
    }

    override fun stop() {
        super.stop()
        reader.stop()
        filteredHeads.clear()
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
        HeadLagObserver(head, upstreams, DistanceExtractor::extractPowDistance, headScheduler, 6).apply {
            start()
        }

    override fun isRunning(): Boolean {
        return super.isRunning() || reader.isRunning()
    }

    override fun getReader(): EthereumCachingReader {
        return reader
    }

    override fun getHead(): Head {
        return head
    }

    override fun tryProxy(
        matcher: Selector.Matcher,
        request: BlockchainOuterClass.NativeSubscribeRequest,
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

    override fun getEgressSubscription(): EgressSubscription {
        return subscribe
    }

    override fun getLocalReader(localEnabled: Boolean): Mono<JsonRpcReader> {
        return Mono.just(EthereumLocalReader(reader, getMethods(), getHead(), localEnabled))
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
                        else -> MergedHead(selected, MostWorkForkChoice(), headScheduler, "Eth head ${it.map { it.getId() }}").apply {
                            start()
                        }
                    }
                }
        }

    override fun getEnrichedHead(mather: Selector.Matcher): Head =
        filteredHeads.computeIfAbsent(mather.describeInternal().intern()) { _ ->
            upstreams.filter { mather.matches(it) }
                .apply {
                    log.debug("Found $size upstreams matching [${mather.describeInternal()}]")
                }.let {
                    val selected = it.map { source -> source.getHead() }
                    EnrichedMergedHead(
                        selected,
                        getHead(),
                        headScheduler,
                        object :
                            Reader<BlockHash, BlockContainer> {
                            override fun read(key: BlockHash): Mono<BlockContainer> {
                                return reader.blocksByHashAsCont().read(key).map { res -> res.data }
                            }
                        },
                    )
                }
        }

    override fun getFeeEstimation(): ChainFees {
        return feeEstimation
    }

    override fun estimateLogsCount(request: BlockchainOuterClass.EstimateLogsCountRequest): Mono<BlockchainOuterClass.EstimateLogsCountResponse> {
        return if (logsEstimation == null) {
            Mono.error(StatusException(Status.UNAVAILABLE.withDescription("Index is not available")))
        } else {
            Mono.just(logsEstimation.estimate(request))
        }
    }
}
