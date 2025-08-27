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

import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.startup.UpstreamChange
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.PreDestroy
import kotlin.concurrent.withLock

@Repository
open class CurrentMultistreamHolder(
    @Autowired private val cachesFactory: CachesFactory,
    @Autowired private val signer: ResponseSigner,
) : MultistreamHolder {
    private val log = LoggerFactory.getLogger(CurrentMultistreamHolder::class.java)

    private val chainMapping = ConcurrentHashMap<Chain, Multistream>()
    private val chainsBus =
        Sinks
            .many()
            .multicast()
            .directBestEffort<Chain>()
    private val addedUpstreams =
        Sinks
            .many()
            .multicast()
            .directBestEffort<Tuple2<Chain, Upstream>>()
    private val callTargets = HashMap<Chain, CallMethods>()
    private val updateLock = ReentrantLock()

    fun update(change: UpstreamChange) {
        updateLock.withLock {
            log.debug("Upstream update: ${change.type} ${change.chain} via ${change.upstream.getId()}")
            val chain = change.chain
            try {
                val current = chainMapping[chain]
                val factory =
                    when (BlockchainType.from(chain)) {
                        BlockchainType.ETHEREUM ->
                            Callable<Multistream> {
                                EthereumMultistream(chain, ArrayList(), cachesFactory.getCaches(chain), signer)
                            }
                        BlockchainType.BITCOIN ->
                            Callable<Multistream> {
                                BitcoinMultistream(chain, ArrayList(), cachesFactory.getCaches(chain), signer)
                            }
                        else -> throw IllegalStateException("Update for unsupported chain: $chain")
                    }
                processUpdate(change, current, factory)
            } catch (e: Throwable) {
                log.error("Failed to update upstream", e)
            }
        }
    }

    fun processUpdate(
        change: UpstreamChange,
        current: Multistream?,
        factory: Callable<Multistream>,
    ) {
        val chain = change.chain
        val up: Upstream = change.upstream
        if (change.type == UpstreamChange.ChangeType.REMOVED) {
            current?.removeUpstream(up.getId())
            log.info("Upstream ${change.upstream.getId()} with chain $chain has been removed")
        } else {
            if (current == null) {
                val created = factory.call()
                if (up is CachesEnabled) {
                    up.setCaches(created.caches)
                }
                created.addUpstream(up)
                created.start()
                chainMapping[chain] = created
                chainsBus.tryEmitNext(chain)
            } else {
                if (up is CachesEnabled) {
                    up.setCaches(current.caches)
                }
                current.addUpstream(up)
            }
            if (!callTargets.containsKey(chain)) {
                setupDefaultMethods(chain)
            }
            log.info("Upstream ${change.upstream.getId()} with chain $chain has been added")
            addedUpstreams.tryEmitNext(Tuples.of(chain, up))
        }
    }

    override fun getUpstream(chain: Chain): Multistream? = chainMapping[chain]

    override fun getAvailable(): List<Chain> = Collections.unmodifiableList(chainMapping.keys.toList())

    override fun observeAddedUpstreams(): Flux<Tuple2<Chain, Upstream>> = Flux.from(addedUpstreams.asFlux())

    override fun observeChains(): Flux<Chain> =
        Flux.concat(
            Flux.fromIterable(getAvailable()),
            chainsBus.asFlux(),
        )

    override fun getDefaultMethods(chain: Chain): CallMethods {
        return callTargets[chain] ?: return setupDefaultMethods(chain)
    }

    fun setupDefaultMethods(chain: Chain): CallMethods {
        val created =
            when (BlockchainType.from(chain)) {
                BlockchainType.ETHEREUM -> DefaultEthereumMethods(chain)
                BlockchainType.BITCOIN -> DefaultBitcoinMethods()
                else -> throw IllegalStateException("Unsupported chain: $chain")
            }
        callTargets[chain] = created
        return created
    }

    override fun isAvailable(chain: Chain): Boolean = chainMapping.containsKey(chain) && callTargets.containsKey(chain)

    @PreDestroy
    fun shutdown() {
        log.info("Closing upstream connections...")
        updateLock.withLock {
            chainMapping.values.forEach {
                it.stop()
            }
            chainMapping.clear()
        }
    }
}
