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

import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.startup.UpstreamChange
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultBitcoinMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.PreDestroy
import kotlin.concurrent.withLock

@Component
open class CurrentMultistreamHolder(
    private val multistreams: List<Multistream>
) : MultistreamHolder {

    private val log = LoggerFactory.getLogger(CurrentMultistreamHolder::class.java)

    private val chainMapping = ConcurrentHashMap<Chain, Multistream>().apply {
        multistreams.forEach { this[it.chain] = it }
    }
    private val chainsBus = Sinks.many()
        .multicast()
        .directBestEffort<Chain>()
    private val callTargets = HashMap<Chain, CallMethods>()
    private val updateLock = ReentrantLock()

    fun update(change: UpstreamChange) {
        updateLock.withLock {
            log.debug("Upstream update: ${change.type} ${change.chain} via ${change.upstream.getId()}")
            val chain = change.chain
            try {
                when (BlockchainType.from(chain)) {
                    BlockchainType.EVM_POW -> {
                        val up = change.upstream.cast(EthereumUpstream::class.java)
                        val current = chainMapping.getValue(chain)
                        processUpdate(change, up, current)
                    }

                    BlockchainType.EVM_POS -> {
                        val up = change.upstream.cast(EthereumPosUpstream::class.java)
                        val current = chainMapping.getValue(chain)
                        processUpdate(change, up, current)
                    }

                    BlockchainType.BITCOIN -> {
                        val up = change.upstream.cast(BitcoinUpstream::class.java)
                        val current = chainMapping.getValue(chain)
                        processUpdate(change, up, current)
                    }

                    else -> {
                        log.error("Update for unsupported chain: $chain")
                    }
                }
            } catch (e: Throwable) {
                log.error("Failed to update upstream", e)
            }
        }
    }

    fun processUpdate(change: UpstreamChange, up: Upstream, current: Multistream) {
        val chain = change.chain
        if (change.type == UpstreamChange.ChangeType.REMOVED) {
            current.removeUpstream(up.getId())
            log.info("Upstream ${change.upstream.getId()} with chain $chain has been removed")
        } else {
            if (up is CachesEnabled) {
                up.setCaches(current.caches)
            }
            current.addUpstream(up)

            if (!callTargets.containsKey(chain)) {
                setupDefaultMethods(chain)
            }
            log.info("Upstream ${change.upstream.getId()} with chain $chain has been added")
        }
    }

    override fun getUpstream(chain: Chain): Multistream? {
        return chainMapping[chain]
    }

    override fun getAvailable(): List<Chain> {
        return multistreams.asSequence()
            .filter { it.isAvailable() }
            .map { it.chain }
            .toList()
    }

    override fun observeChains(): Flux<Chain> {
        return Flux.concat(
            Flux.fromIterable(getAvailable()),
            chainsBus.asFlux()
        )
    }

    override fun getDefaultMethods(chain: Chain): CallMethods {
        return callTargets[chain] ?: return setupDefaultMethods(chain)
    }

    fun setupDefaultMethods(chain: Chain): CallMethods {
        val created = when (BlockchainType.from(chain)) {
            BlockchainType.EVM_POW -> DefaultEthereumMethods(chain)
            BlockchainType.BITCOIN -> DefaultBitcoinMethods()
            BlockchainType.EVM_POS -> DefaultEthereumMethods(chain)
            else -> throw IllegalStateException("Unsupported chain: $chain")
        }
        callTargets[chain] = created
        return created
    }

    override fun isAvailable(chain: Chain): Boolean {
        return chainMapping.containsKey(chain) && callTargets.containsKey(chain)
    }

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
