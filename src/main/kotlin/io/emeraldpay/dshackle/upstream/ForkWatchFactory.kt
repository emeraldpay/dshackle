/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.api.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.EnumMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class ForkWatchFactory(
    @Autowired private val currentMultistreamHolder: CurrentMultistreamHolder,
) {

    companion object {
        private val log = LoggerFactory.getLogger(ForkWatchFactory::class.java)
    }

    private val initialized = EnumMap<Chain, ForkWatch>(Chain::class.java)
    private val initializeLock = ReentrantLock()

    private val posChains = listOf(
        // at this moment (Aug 2022) it's still a PoW, but upgrade is coming in weeks, so it's better to configure everything in advance
        Chain.ETHEREUM,
        // those are upgraded to Merge
        Chain.TESTNET_GOERLI, Chain.TESTNET_ROPSTEN
    )

    fun create(chain: Chain): ForkWatch {
        return initializeLock.withLock {
            initialized.getOrPut(chain) {
                val forkChoice = if (posChains.contains(chain)) {
                    PriorityForkChoice().also {
                        it.followUpstreams(
                            currentMultistreamHolder.observeAddedUpstreams()
                                .filter { it.t1 == chain }
                                .map { it.t2 }
                        )
                    }
                } else {
                    DifficultyForkChoice()
                }
                ForkWatch(forkChoice, chain)
            }
        }
    }
}
