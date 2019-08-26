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

import io.emeraldpay.dshackle.quorum.CallQuorum
import java.util.*
import kotlin.collections.HashSet

class AggregatedCallMethods(
        private val delegates: Collection<CallMethods>
): CallMethods {

    private val allMethods: Set<String>

    init {
        val buf = HashSet<String>()
        delegates.map { it.getSupportedMethods().forEach { m -> buf.add(m) } }
        allMethods = Collections.unmodifiableSet(buf)
    }

    override fun getQuorumFor(method: String): CallQuorum {
        return delegates.find {
            it.isAllowed(method)
        }?.getQuorumFor(method) ?: throw IllegalStateException("No quorum for $method")
    }

    override fun isAllowed(method: String): Boolean {
        return delegates.any { it.isAllowed(method) }
    }

    override fun getSupportedMethods(): Set<String> {
        return allMethods
    }

    override fun isHardcoded(method: String): Boolean {
        return delegates.any { it.isAllowed(method) && it.isHardcoded(method) }
    }

    override fun executeHardcoded(method: String): Any {
        return delegates.find {
            it.isAllowed(method) && it.isHardcoded(method)
        }?.executeHardcoded(method) ?: throw IllegalStateException("No hardcoded for $method")
    }
}