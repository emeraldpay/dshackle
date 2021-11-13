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
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.grpc.Chain
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
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

    /**
     * Initiate a new head subscription with connection to the remote
     */
    private fun internalStart(remote: ReactorBlockchainGrpc.ReactorBlockchainStub) {
        if (this.isRunning) {
            stop()
        }
        log.debug("Start Head subscription to ${parent.getId()}")

        val source = Flux.concat(
            // first connect immediately
            Flux.just(remote),
            // following requests do with delay, give it a time to recover
            Flux.just(remote).repeat().delayElements(Defaults.retryConnection)
        ).flatMap(this::subscribeHead)

        internalStart(source)
    }

    fun subscribeHead(client: ReactorBlockchainGrpc.ReactorBlockchainStub): Publisher<BlockchainOuterClass.ChainHead> {
        val chainRef = Common.Chain.newBuilder()
            .setTypeValue(chain.id)
            .build()
        return client.subscribeHead(chainRef)
            // simple retry on failure, if eventually failed then it supposed to resubscribe later from outer method
            .retryWhen(Retry.backoff(4, Duration.ofSeconds(1)))
            .onErrorContinue { err, _ ->
                log.warn("Disconnected $chain from ${parent.getId()}: ${err.message}")
                parent.setStatus(UpstreamAvailability.UNAVAILABLE)
                Mono.empty<BlockchainOuterClass.ChainHead>()
            }
    }

    /**
     * Initiate a new head from provided source of head details
     */
    private fun internalStart(source: Flux<BlockchainOuterClass.ChainHead>) {
        var blocks = source.map(converter)
            .distinctUntilChanged {
                it.hash
            }.filter { block ->
                val curr = this.getCurrent()
                curr == null || curr.difficulty < block.difficulty
            }
        if (enhancer != null) {
            blocks = blocks.flatMap(enhancer)
        }

        blocks = blocks.onErrorContinue { err, _ ->
            log.error("Head subscription error. ${err.javaClass.name}:${err.message}", err)
        }

        headSubscription = super.follow(blocks)
    }

    override fun isRunning(): Boolean {
        return !(headSubscription?.isDisposed ?: true)
    }

    override fun start() {
        this.internalStart(remote)
    }

    override fun stop() {
        headSubscription?.dispose()
    }
}
