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
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.startup.UpstreamChange
import io.emeraldpay.dshackle.upstream.bitcoin.DirectBitcoinApi
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinChainUpstreams
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainUpstreams
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Repository
class CurrentUpstreams(
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val cachesFactory: CachesFactory
): Upstreams {

    private val log = LoggerFactory.getLogger(CurrentUpstreams::class.java)

    private val chainMapping = ConcurrentHashMap<Chain, ChainUpstreams<*>>()
    private val chainsBus = TopicProcessor.create<Chain>()
    private val callTargets = HashMap<Chain, CallMethods>()
    private val updateLock = ReentrantLock()

    fun update(change: UpstreamChange) {
        updateLock.withLock {
            val chain = change.chain
            when (BlockchainType.fromBlockchain(chain)) {
                BlockchainType.ETHEREUM -> {
                    val up = change.upstream
                            .cast(EthereumUpstream::class.java, EthereumApi::class.java) as Upstream<EthereumApi>
                    val current = chainMapping[chain] as ChainUpstreams<EthereumApi>?
                    val factory = Callable {
                        EthereumChainUpstreams(chain, ArrayList(), cachesFactory.getCaches(chain), objectMapper) as ChainUpstreams<EthereumApi>
                    }
                    processUpdate(change, up, current, factory)
                }
                BlockchainType.BITCOIN -> {
                    val up = change.upstream
                            .cast(BitcoinUpstream::class.java, DirectBitcoinApi::class.java)
                    val current = chainMapping[chain] as ChainUpstreams<DirectBitcoinApi>?
                    val factory = Callable {
                        BitcoinChainUpstreams(chain, ArrayList(), cachesFactory.getCaches(chain), objectMapper) as ChainUpstreams<DirectBitcoinApi>
                    }
                    processUpdate(change, up, current, factory)
                }
                else -> {
                    log.error("Update for unsupported chain: $chain")
                }
            }
        }
    }

    fun <A : UpstreamApi> processUpdate(change: UpstreamChange, up: Upstream<A>, current: ChainUpstreams<A>?, factory: Callable<ChainUpstreams<A>>) {
        val chain = change.chain
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
                chainsBus.onNext(chain)
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
        }
    }

    override fun getUpstream(chain: Chain): AggregatedUpstream<*>? {
        return chainMapping[chain]
    }

    @Scheduled(fixedRate = 15000)
    fun printStatuses() {
        chainMapping.forEach { it.value.printStatus() }
    }

    override fun getAvailable(): List<Chain> {
        return Collections.unmodifiableList(chainMapping.keys.toList())
    }

    override fun observeChains(): Flux<Chain> {
        return Flux.merge(
                Flux.fromIterable(getAvailable()),
                Flux.from(chainsBus)
        )
    }

    override fun getDefaultMethods(chain: Chain): CallMethods {
        return callTargets[chain] ?: return setupDefaultMethods(chain)
    }

    fun setupDefaultMethods(chain: Chain): CallMethods {
        val created = when (BlockchainType.fromBlockchain(chain)) {
            BlockchainType.ETHEREUM -> DefaultEthereumMethods(objectMapper, chain)
            BlockchainType.BITCOIN -> DefaultBitcoinMethods(objectMapper)
            else -> throw IllegalStateException("Unsupported chain: $chain")
        }
        callTargets[chain] = created
        return created
    }

    override fun isAvailable(chain: Chain): Boolean {
        return chainMapping.containsKey(chain) && callTargets.containsKey(chain)
    }
}