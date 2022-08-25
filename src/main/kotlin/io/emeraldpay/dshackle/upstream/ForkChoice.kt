/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.data.BlockContainer

/**
 * Decision for finding out a status of a head of the upstream considering the whole state of the blockchain
 */
interface ForkChoice {

    /**
     * Submit the latest block on the upstream to find out the upstream status compared to other existing
     * upstreams on the same blockchain
     */
    fun submit(block: BlockContainer, upstream: Upstream): Status

    /**
     * The name of the fork choice implementation for logging purposes
     */
    fun getName(): String

    enum class Status {
        /**
         * The upstream just reached a new good block
         */
        NEW,

        /**
         * The upstream reached a new block, but the other upstreams are not aware of that block yet.
         * I.e., the block still may go away / be replaced
         */
        OUTRUN,

        /**
         * The upstream just received the same block as other upstreams
         */
        EQUAL,

        /**
         * The upstream has a known block, but it's still behind the other upstreams
         */
        FALLBEHIND,

        /**
         * The upstream has a block on a rejected chain
         */
        REJECTED;

        /**
         * true if it's on the main chain
         */
        val isOk: Boolean
            get() = this != REJECTED
    }
}
