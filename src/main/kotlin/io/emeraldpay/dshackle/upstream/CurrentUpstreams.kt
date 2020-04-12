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
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.cache.CachesFactory
import io.emeraldpay.dshackle.startup.UpstreamChange
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainUpstreams
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Repository
class CurrentUpstreams(
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val cachesFactory: CachesFactory
): Upstreams {

    private val log = LoggerFactory.getLogger(CurrentUpstreams::class.java)

    private val chainMapping = ConcurrentHashMap<Chain, ChainUpstreams<*, *>>()
    private val chainsBus = TopicProcessor.create<Chain>()
    private val callTargets = HashMap<Chain, CallMethods>()
    private val updateLock = ReentrantLock()

    fun update(change: UpstreamChange) {
        updateLock.withLock {
            val chain = change.chain
            val up = change.upstream
                    .cast(EthereumUpstream::class.java, EthereumApi::class.java, BlockJson::class.java) as Upstream<EthereumApi, BlockJson<TransactionRefJson>>
            val current = chainMapping[chain] as ChainUpstreams<EthereumApi, BlockJson<TransactionRefJson>>?
            if (change.type == UpstreamChange.ChangeType.REMOVED) {
                current?.removeUpstream(up.getId())
                log.info("Upstream ${change.upstream.getId()} with chain $chain has been removed")
            } else {
                if (current == null) {
                    val created = EthereumChainUpstreams(chain, ArrayList(), cachesFactory.getCaches(chain), objectMapper)
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
    }

    override fun getUpstream(chain: Chain): AggregatedUpstream<*, *>? {
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
        val created = DefaultEthereumMethods(objectMapper, chain)
        callTargets[chain] = created
        return created
    }

    override fun isAvailable(chain: Chain): Boolean {
        return chainMapping.containsKey(chain) && callTargets.containsKey(chain)
    }
}