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
package io.emeraldpay.dshackle.startup

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.grpc.Chain

/**
 * An update event to the list of currently available upstreams.
 */
class UpstreamChange(
        /**
         * Target blockchain
         */
        val chain: Chain,
        /**
         * Corresponding upstream
         */
        val upstream: Upstream<*, *>,
        /**
         * Type of the change
         */
        val type: ChangeType
): CachesEnabled {

    enum class ChangeType {
        /**
         * Upstream just added
         */
        ADDED,

        /**
         * Upstream become available after being temporally off
         */
        REVALIDATED,

        /**
         * Upstream is removed (it still doesn't mean it wouldn't return again after some reconfiguration)
         */
        REMOVED,
    }

    override fun setCaches(caches: Caches) {
        if (upstream is CachesEnabled) {
            upstream.setCaches(caches)
        }
    }
}