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
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.kotlin.core.publisher.switchIfEmpty
import kotlin.math.abs

open class GenericHead(
    protected val upstreamId: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    private val headScheduler: Scheduler,
    private val chainSpecific: ChainSpecific,
) : Head, AbstractHead(forkChoice, headScheduler, blockValidator, 60_000, upstreamId) {
    protected val headLivenessSink: Sinks.Many<HeadLivenessState> = Sinks.many().multicast().directBestEffort()

    fun getLatestBlock(api: ChainReader): Mono<BlockContainer> {
        return chainSpecific.getLatestBlock(api, upstreamId)
            .subscribeOn(headScheduler)
            .timeout(Defaults.timeout, Mono.error(Exception("Block data not received")))
            .onErrorResume { err ->
                log.error("Failed to fetch latest block: ${err.message} $upstreamId", err)
                Mono.empty()
            }
    }

    override fun isSuspiciousBlock(block: BlockContainer): Boolean {
        return getCurrentHeight()
            ?.let {
                abs(block.height - it) > 10000
            } ?: false
    }

    override fun checkSuspiciousBlock(block: BlockContainer): Mono<BlockContainer> {
        return Mono.justOrEmpty(chainIdValidator())
            .flatMap {
                it!!.validate(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
            }
            .switchIfEmpty {
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
            }
            .flatMap { validationResult ->
                when (validationResult) {
                    ValidateUpstreamSettingsResult.UPSTREAM_VALID -> {
                        log.info("Block {} of upstream {} has been received from the same chain, validation is passed", block.height, upstreamId)
                        Mono.just(block)
                    }
                    ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR -> {
                        log.warn("Block {} of upstream {} is filtered and can not be emitted due to upstream settings error check", block.height, upstreamId)
                        Mono.empty()
                    }
                    ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR -> {
                        log.error("Block {} of upstream {} can not be emitted due to chain inconsistency, upstream will be removed", block.height, upstreamId)
                        headLivenessSink.emitNext(HeadLivenessState.FATAL_ERROR) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                        Mono.empty()
                    }
                }
            }
    }

    override fun headLiveness(): Flux<HeadLivenessState> = headLivenessSink.asFlux()

    protected open fun chainIdValidator(): SingleValidator<ValidateUpstreamSettingsResult>? {
        return null
    }
}
