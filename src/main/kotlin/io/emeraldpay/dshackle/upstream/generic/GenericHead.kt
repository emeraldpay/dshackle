/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

open class GenericHead(
    protected val upstreamId: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    private val headScheduler: Scheduler,
    private val chainSpecific: ChainSpecific,
) : Head, AbstractHead(forkChoice, headScheduler, blockValidator, 60_000, upstreamId) {

    fun getLatestBlock(api: JsonRpcReader): Mono<BlockContainer> {
        return api.read(chainSpecific.latestBlockRequest())
            .subscribeOn(headScheduler)
            .timeout(Defaults.timeout, Mono.error(Exception("Block data not received")))
            .map { chainSpecific.parseBlock(it, upstreamId) }
            .onErrorResume { err ->
                log.error("Failed to fetch latest block: ${err.message} $upstreamId", err)
                Mono.empty()
            }
    }
}
