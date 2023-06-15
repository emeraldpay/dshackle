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

import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.commons.DurableFlux
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import org.springframework.util.backoff.ExponentialBackOff
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Function

class GrpcHead(
    private val chain: Chain,
    private val parent: DefaultUpstream,
    private val remote: ReactorBlockchainGrpc.ReactorBlockchainStub,
    /**
     * Converted from remote head details to the block container, which could be partial at this point
     */
    private val converter: Function<BlockchainOuterClass.ChainHead, BlockContainer>,
    /**
     * Populate block data with all missing details, of any
     */
    private val enhancer: Function<BlockContainer, Publisher<BlockContainer>>?
) : AbstractHead(), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(GrpcHead::class.java)
    }

    private var headSubscription: Disposable? = null
    private val shouldBeRunning = AtomicBoolean(false)

    /**
     * Initiate a new head subscription with connection to the remote
     */
    private fun internalStart(remote: ReactorBlockchainGrpc.ReactorBlockchainStub) {
        if (this.isRunning) {
            stop()
        }
        log.debug("Start Head subscription to ${parent.getId()}")

        val blocks = DurableFlux(
            { connect(remote) },
            ExponentialBackOff(100, 1.5),
            log,
            shouldBeRunning,
        )
        headSubscription = super.follow(blocks.connect())
    }

    private fun connect(remote: ReactorBlockchainGrpc.ReactorBlockchainStub): Flux<BlockContainer> {
        val chainRef = Common.Chain.newBuilder()
            .setTypeValue(chain.id)
            .build()
        return remote.subscribeHead(chainRef)
            // if nothing returned for a relatively long period it's probably because of a broken connection, so in this case we force to drop the connection
            .timeout(
                expectEventsTime(),
                Mono.fromCallable { log.info("No events received from ${parent.getId()}. Reconnecting...") }
                    .then(Mono.error(SilentException.Timeout("No Events")))
            )
            .doOnError { err ->
                if (err !is SilentException) {
                    log.warn("Disconnected $chain from ${parent.getId()}: ${err.message}")
                }
                parent.setStatus(UpstreamAvailability.UNAVAILABLE)
            }
            .map(converter)
            .distinctUntilChanged(BlockContainer::hash)
            .transform(enhanced())
    }

    private fun expectEventsTime(): Duration {
        return try {
            when (BlockchainType.from(chain)) {
                BlockchainType.BITCOIN -> Duration.ofHours(1)
                BlockchainType.ETHEREUM -> Duration.ofMinutes(5)
            }
        } catch (e: IllegalArgumentException) {
            Duration.ofMinutes(15)
        }
    }

    private fun enhanced(): Function<Flux<BlockContainer>, Flux<BlockContainer>> {
        return if (enhancer != null) {
            Function { blocks -> blocks.flatMap(enhancer) }
        } else {
            Function.identity()
        }
    }

    override fun isRunning(): Boolean {
        return !(headSubscription?.isDisposed ?: true)
    }

    override fun start() {
        headSubscription?.dispose()
        shouldBeRunning.set(true)
        this.internalStart(remote)
    }

    override fun stop() {
        shouldBeRunning.set(false)
        headSubscription?.dispose()
    }
}
