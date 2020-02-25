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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import java.util.*

/**
 * Wrapper on top of another configuration, that may disable or enable additional methods on top of it.
 * If a new method enabled, then its Quorum will be [AlwaysQuorum]. For other methods it delegates all to the provided
 * parent config.
 */
class ManagedCallMethods(
        private val delegate: CallMethods,
        private val enabled: Set<String>,
        private val disabled: Set<String>
): CallMethods {

    private val allAllowed: Set<String> = Collections.unmodifiableSet(
            enabled + delegate.getSupportedMethods() - disabled
    )

    override fun getQuorumFor(method: String): CallQuorum {
        return if (enabled.contains(method)) {
            AlwaysQuorum()
        } else {
            delegate.getQuorumFor(method)
        }
    }

    override fun isAllowed(method: String): Boolean {
        return allAllowed.contains(method)
    }

    override fun getSupportedMethods(): Set<String> {
        return allAllowed
    }

    override fun isHardcoded(method: String): Boolean {
        return delegate.isHardcoded(method)
    }

    override fun executeHardcoded(method: String): Any {
        return delegate.executeHardcoded(method)
    }
}