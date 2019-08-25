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

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum

class DirectCallMethods : CallMethods {

    override fun getQuorumFor(method: String): CallQuorum {
        return AlwaysQuorum()
    }

    override fun isAllowed(method: String): Boolean {
        return true
    }

    override fun getSupportedMethods(): Set<String> {
        return emptySet()
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun hardcoded(method: String): Any {
        return "unsupported"
    }
}