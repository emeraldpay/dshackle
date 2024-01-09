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
package io.emeraldpay.dshackle.startup

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.upstream.Upstream

/**
 * An update event to the list of currently available upstreams.
 */
data class UpstreamChangeEvent(
    /**
     * Target blockchain
     */
    val chain: Chain,
    /**
     * Corresponding upstream
     */
    val upstream: Upstream,
    /**
     * Type of the change
     */
    val type: ChangeType,
) : CachesEnabled {

    enum class ChangeType {
        /**
         * Upstream just added
         */
        ADDED,

        /**
         * Some upstream details changed
         */
        UPDATED,

        /**
         * Upstream become available after being temporally off
         */
        REVALIDATED,

        /**
         * Upstream is removed (it still doesn't mean it wouldn't return again after some reconfiguration)
         */
        REMOVED,

        /**
         * Upstream is removed but not stopped due to upstream fatal settings error (it could return some reconfiguration)
         */
        FATAL_SETTINGS_ERROR_REMOVED,

        /**
         * Upstream is observed for state updates (it could be added later)
         */
        OBSERVED,
    }

    override fun setCaches(caches: Caches) {
        if (upstream is CachesEnabled) {
            upstream.setCaches(caches)
        }
    }

    override fun toString(): String {
        return "UpstreamChangeEvent(chain=$chain, upstream=${upstream.getId()}, type=$type)"
    }
}
