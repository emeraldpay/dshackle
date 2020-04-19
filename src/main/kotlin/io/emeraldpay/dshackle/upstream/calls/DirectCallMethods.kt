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

/**
 * Configuration that uses [AlwaysQuorum] for all available methods. The methods list itself
 * is provided in constructor (or empty otherwise)
 */
open class DirectCallMethods(private val methods: Set<String>) : CallMethods {

    constructor() : this(emptySet())
    constructor(methods: Collection<String>) : this(methods.toSet())

    override fun getQuorumFor(method: String): CallQuorum {
        return AlwaysQuorum()
    }

    override fun isAllowed(method: String): Boolean {
        return methods.contains(method)
    }

    override fun getSupportedMethods(): Set<String> {
        return methods
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun executeHardcoded(method: String): Any {
        return "unsupported"
    }
}