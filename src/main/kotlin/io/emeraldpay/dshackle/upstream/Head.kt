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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Subscription to listen to updates to the head of a blockchain.
 */
interface Head {

    /**
     * @return stream of all new blocks, starts from the current block (i.e., first item should be available immediately).
     */
    fun getFlux(): Flux<BlockContainer>

    /**
     * Add handler that is going to be called each time _before_ a new block is submitted to stream of new blocks.
     * Supposed to be used for cleanup/preparation before actual block data will come, to avoid race condition.
     * @see getFlux
     */
    fun onBeforeBlock(handler: Runnable)

    fun getCurrentHeight(): Long?
}