/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle

import io.emeraldpay.api.proto.Common
import io.emeraldpay.grpc.Chain
import java.util.EnumMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Keeps a lazily created value associated with a Chain
 */
class ChainValue<V>(
    private val factory: (chain: Chain) -> V
) {

    private val values = EnumMap<Chain, V>(Chain::class.java)
    private val valuesInitLock = ReentrantLock()

    fun get(chain: Common.ChainRef): V {
        return get(Chain.byId(chain.number))
    }

    fun get(chain: Chain): V {
        val existing = values[chain]
        if (existing != null) {
            return existing
        }
        return valuesInitLock.withLock {
            // second check in case it was updated while getting the lock
            val existing2 = values[chain]
            if (existing2 != null) {
                existing2
            } else {
                val created = factory(chain)
                values[chain] = created
                created
            }
        }
    }
}
