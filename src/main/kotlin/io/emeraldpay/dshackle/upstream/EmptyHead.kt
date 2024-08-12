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
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import reactor.core.publisher.Flux

class EmptyHead : Head {

    override fun getFlux(): Flux<BlockContainer> {
        return Flux.empty()
    }

    override fun onBeforeBlock(handler: Runnable) {
    }

    override fun getCurrentHeight(): Long? {
        return null
    }

    override fun getCurrentSlotHeight(): Long? {
        return null
    }
    override fun start() {
    }

    override fun stop() {
    }

    override fun onSyncingNode(isSyncing: Boolean) {
    }

    override fun headLiveness(): Flux<HeadLivenessState> = Flux.empty()

    override fun getCurrent(): BlockContainer? {
        return null
    }
}
