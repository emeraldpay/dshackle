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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.CallQuorum
import java.util.*
import kotlin.collections.HashSet

/**
 * Aggregation over several parent configuration. It dispatches call to a first delegate that supports it.
 */
class AggregatedCallMethods(
        private val delegates: Collection<CallMethods>
): CallMethods {

    private val allMethods: Set<String>

    init {
        val buf = HashSet<String>()
        delegates.map { it.getSupportedMethods().forEach { m -> buf.add(m) } }
        allMethods = Collections.unmodifiableSet(buf)
    }

    /**
     * Finds first delegate that has Allowed that method and returns its Quorum
     */
    override fun getQuorumFor(method: String): CallQuorum {
        return delegates.find {
            it.isAllowed(method)
        }?.getQuorumFor(method) ?: throw IllegalStateException("No quorum for $method")
    }

    /**
     * Checks if ANY of delegates supports the method
     */
    override fun isAllowed(method: String): Boolean {
        return delegates.any { it.isAllowed(method) }
    }

    /**
     * Returns all available methods, accessible through at least one of delegates
     */
    override fun getSupportedMethods(): Set<String> {
        return allMethods
    }

    /**
     * @return true if there is at least one delegate that allows the method and it's hardcoded on that delegate
     */
    override fun isHardcoded(method: String): Boolean {
        return delegates.any { it.isAllowed(method) && it.isHardcoded(method) }
    }

    /**
     * Executed the method on the first delegate that supports it as a hardcoded method
     */
    override fun executeHardcoded(method: String): ByteArray {
        return delegates.find {
            it.isAllowed(method) && it.isHardcoded(method)
        }?.executeHardcoded(method) ?: throw IllegalStateException("No hardcoded for $method")
    }
}