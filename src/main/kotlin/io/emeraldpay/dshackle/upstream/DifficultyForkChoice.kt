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

import io.emeraldpay.dshackle.data.BlockContainer
import org.slf4j.LoggerFactory
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * A Fork Choice which takes Total Difficulty into account. Can work only with Proof-of-Work blockchains.
 * Note that the current implementation doesn't check for actual state of the forks, and makes decision on the Total Difficulty order exclusively
 */
class DifficultyForkChoice : ForkChoice {

    companion object {
        private val log = LoggerFactory.getLogger(DifficultyForkChoice::class.java)
    }

    private val current = AtomicReference<BigInteger>(BigInteger.ZERO)

    override fun submit(block: BlockContainer, upstream: Upstream): ForkChoice.Status {
        val difficulty = block.difficulty
        val previous = current.getAndUpdate {
            if (it < difficulty) {
                difficulty
            } else {
                it
            }
        }
        return when {
            previous > difficulty -> ForkChoice.Status.FALLBEHIND
            previous < difficulty -> ForkChoice.Status.NEW
            else -> ForkChoice.Status.EQUAL
        }
    }

    override fun getName(): String {
        return "Difficulty"
    }
}
