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
import java.util.concurrent.atomic.AtomicLong

class NeverForkChoice : ForkChoice {

    companion object {
        private val log = LoggerFactory.getLogger(NeverForkChoice::class.java)
    }

    private val height = AtomicLong(0)

    override fun submit(block: BlockContainer, upstream: Upstream): ForkChoice.Status {
        val prev = height.getAndUpdate {
            it.coerceAtLeast(block.height)
        }
        return when {
            prev > block.height -> ForkChoice.Status.FALLBEHIND
            prev < block.height -> ForkChoice.Status.NEW
            else -> ForkChoice.Status.EQUAL
        }
    }

    override fun getName(): String {
        return "Never"
    }
}
