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

enum class UpstreamAvailability(val grpcId: Int) {

    /**
     * Active fully synchronized node
     */
    OK(1),
    /**
     * Good node, but is still synchronizing a latest block
     */
    LAGGING(2),
    /**
     * May be good, but node doesn't have enough peers to be sure it's on corrected chain
     */
    IMMATURE(3),
    /**
     * Node is doing it's initial synchronization, is behind by at least several blocks
     */
    SYNCING(4),
    /**
     * Unavailable node
     */
    UNAVAILABLE(5);

    companion object {
        fun fromGrpc(id: Int?): UpstreamAvailability {
            if (id == null) {
                return UNAVAILABLE
            }
            return UpstreamAvailability.values().find {
                it.grpcId == id
            } ?: UNAVAILABLE
        }
    }
}