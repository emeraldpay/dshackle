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
package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import org.reactivestreams.Publisher
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.kotlin.extra.retry.retryExponentialBackoff
import java.time.Duration
import java.util.function.Function

class GrpcHead(
    id: String,
    private val chain: Chain,
    private val parent: DefaultUpstream,
    private val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
    /**
     * Converted from remote head details to the block container, which could be partial at this point
     */
    private val converter: Function<BlockchainOuterClass.ChainHead, GrpcHeadData>,
    /**
     * Populate block data with all missing details, of any
     */
    private val enhancer: Function<BlockContainer, Publisher<BlockContainer>>?,
    private val forkChoice: ForkChoice,
    headScheduler: Scheduler,
) : AbstractHead(forkChoice, headScheduler, upstreamId = id), Lifecycle {

    private var headSubscription: Disposable? = null

    private val lowerBoundsSink = Sinks.many().multicast().directBestEffort<LowerBoundData>()

    /**
     * Initiate a new head subscription with connection to the remote
     */
    private fun internalStart(remote: ReactorBlockchainGrpc.ReactorBlockchainStub) {
        if (this.isRunning()) {
            stop()
        }
        log.debug("Start Head subscription to ${parent.getId()}")
        subscribeHead(remote)
    }

    fun subscribeHead(client: ReactorBlockchainGrpc.ReactorBlockchainStub) {
        val chainRef = Common.Chain.newBuilder()
            .setTypeValue(chain.id)
            .build()

        val heads =
            Flux.concat(
                Mono.just(client),
                Mono.just(client)
                    .repeat()
                    .delayElements(Duration.ofSeconds(1)),
            )
                .concatMap({ it.subscribeHead(chainRef) }, 0)
                .doOnNext {
                    headsCounter.increment()
                }
                .sample(Duration.ofMillis(1)) // protect from too many heads
                .doFinally {
                    parent.setStatus(UpstreamAvailability.UNAVAILABLE)
                    log.warn("Head subscription finished: $it")
                }

        var blocks = heads.map(converter)
            .doOnNext {
                it.lowerBounds.forEach { bound -> lowerBoundsSink.tryEmitNext(bound) }
            }
            .map { it.block }
            .distinctUntilChanged {
                it.hash
            }.filter { forkChoice.filter(it) }

        if (enhancer != null) {
            blocks = blocks.flatMap(enhancer)
        }

        blocks = blocks.doOnNext {
            log.trace("Received block ${it.height}")
        }.retryExponentialBackoff(
            Long.MAX_VALUE,
            Duration.ofMillis(100),
            Duration.ofSeconds(60),
            true,
        ) {
            log.debug("Retry grpc head connection ${parent.getId()}")
        }

        headSubscription = super.follow(blocks)
    }

    override fun isRunning(): Boolean {
        return !(headSubscription?.isDisposed ?: true)
    }

    override fun start() {
        super.start()
        this.internalStart(remote)
    }

    override fun stop() {
        super.stop()
        headSubscription?.dispose()
    }

    fun lowerBoundsFlux(): Flux<LowerBoundData> = lowerBoundsSink.asFlux()

    val headsCounter = Counter.builder("grpc_head_received")
        .tag("upstream", id)
        .tag("chain", chain.chainCode)
        .register(Metrics.globalRegistry)

    data class GrpcHeadData(
        val block: BlockContainer,
        val lowerBounds: List<LowerBoundData>,
    ) {
        constructor(block: BlockContainer) : this(block, emptyList())
    }
}
