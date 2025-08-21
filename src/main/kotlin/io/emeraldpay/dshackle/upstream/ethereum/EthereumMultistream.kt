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

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.MultistreamReader
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.HardcodedReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IntegralRpcReader
import io.emeraldpay.dshackle.upstream.MergedPosHead
import io.emeraldpay.dshackle.upstream.MergedPowHead
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.VerifyingReader
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.AggregatedPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.NoPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

@Suppress("UNCHECKED_CAST")
open class EthereumMultistream(
    chain: Chain,
    val upstreams: MutableList<EthereumUpstream>,
    caches: Caches,
    signer: ResponseSigner,
) : Multistream(chain, upstreams as MutableList<Upstream>, caches) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumMultistream::class.java)
    }

    private var currentHead = AtomicReference<Head>(EmptyHead())

    private val ingressReader = CompoundReader(CacheReader(caches), MultistreamReader(this, signer))
    val dataReaders = DataReaders(ingressReader, currentHead)
    private val normalizedReader = NormalizingReader(currentHead, caches, EthereumFullBlocksReader(dataReaders))
    private val ingressFinalReader = CompoundReader(normalizedReader, ingressReader)

    private val reader = IntegralRpcReader(
        VerifyingReader(callMethods),
        HardcodedReader(callMethods),
        ingressFinalReader
    )

    private var subscribe = EthereumEgressSubscription(this, NoPendingTxes())

    private val supportsEIP1559 = when (chain) {
        Chain.ETHEREUM, Chain.TESTNET_ROPSTEN, Chain.TESTNET_GOERLI, Chain.TESTNET_HOLESKY, Chain.TESTNET_SEPOLIA, Chain.TESTNET_RINKEBY -> true
        else -> false
    }

    private val isPos = when (chain) {
        Chain.ETHEREUM, Chain.TESTNET_GOERLI, Chain.TESTNET_HOLESKY, Chain.TESTNET_SEPOLIA -> true
        else -> false
    }

    private val feeEstimation = if (supportsEIP1559) {
        EthereumPriorityFees(this, dataReaders, 256)
    } else {
        EthereumLegacyFees(this, dataReaders, 256)
    }

    init {
        this.init()
    }

    override fun init() {
        if (upstreams.size > 0) {
            currentHead.set(updateHead())
        }
        super.init()
    }

    override fun onUpstreamsUpdated() {
        super.onUpstreamsUpdated()

        val pendingTxes: PendingTxesSource? = upstreams
            .mapNotNull {
                it.getIngressSubscription().getPendingTxes()
            }.let {
                if (it.isEmpty()) {
                    null
                } else if (it.size == 1) {
                    it.first()
                } else {
                    AggregatedPendingTxes(it)
                }
            }
        subscribe = EthereumEgressSubscription(this, pendingTxes)
    }

    override fun getHead(): Head {
        return currentHead.get()
    }

    override fun setHead(head: Head) {
        this.currentHead.set(head)
        onHeadUpdated(head)
    }

    override fun updateHead(): Head {
        lagObserver?.stop()
        lagObserver = null
        val head = if (upstreams.isEmpty()) {
            log.warn("No upstreams set")
            EmptyHead()
        } else if (upstreams.size == 1) {
            val upstream = upstreams.first()
            upstream.setLag(0)
            upstream.getHead().apply {
                if (this is Lifecycle && !this.isRunning) {
                    this.start()
                }
            }
        } else {
            val newHead = if (isPos) {
                val heads = upstreams.map { Pair(it.getOptions().priority, it.getHead()) }
                MergedPosHead(heads)
            } else {
                val heads = upstreams.map { it.getHead() }
                MergedPowHead(heads)
            }.apply {
                this.start()
            }
            val lagObserver = EthereumHeadLagObserver(newHead, upstreams as Collection<Upstream>)
            this.lagObserver = lagObserver
            lagObserver.start()
            newHead
        }
        onHeadUpdated(head)
        return head
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Multistream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        return reader.read(key)
    }

    override fun getEgressSubscription(): EthereumEgressSubscription {
        return subscribe
    }

    override fun getFeeEstimation(): ChainFees {
        return feeEstimation
    }
}
