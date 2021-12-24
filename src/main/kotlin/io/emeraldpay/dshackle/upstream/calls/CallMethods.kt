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

/**
 * Configuration for upstream calls
 */
interface CallMethods {

    /**
     * @return CallQuorum configured for the specified method
     */
    fun getQuorumFor(method: String): CallQuorum

    /**
     * Check if the method can be called on an upstream. Doesn't include Hardcoded methods
     *
     * @return false if call for that method is not allowed.
     * @see isHardcoded
     * @see isAvailable
     */
    fun isCallable(method: String): Boolean

    /**
     * @return list of all allowed methods.
     */
    fun getSupportedMethods(): Set<String>

    /**
     * @return true if the method should not be executed on upstream, but accessed through this class
     */
    fun isHardcoded(method: String): Boolean

    /**
     * Read [supposed to be predefined] method from this config
     */
    fun executeHardcoded(method: String): ByteArray

    /**
     * Check if the method is available either by an upstream or as a hardcoded response
     */
    fun isAvailable(method: String): Boolean {
        return isCallable(method) || isHardcoded(method)
    }
}
