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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.generic.ChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericHead
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class GenericWsHead(
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    private val api: JsonRpcReader,
    private val wsSubscriptions: WsSubscriptions,
    private val wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler,
    upstream: DefaultUpstream,
    private val chainSpecific: ChainSpecific,
) : GenericHead(upstream.getId(), forkChoice, blockValidator, headScheduler, chainSpecific), Lifecycle {

    private var connectionId: String? = null
    private var subscribed = false
    private var connected = false
    private var isSyncing = false

    private var subscription: Disposable? = null
    private val noHeadUpdatesSink = Sinks.many().multicast().directBestEffort<Boolean>()
    private val headLivenessSink = Sinks.many().multicast().directBestEffort<Boolean>()

    private var subscriptionId = AtomicReference("")

    init {
        registerHeadResubscribeFlux()
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        super.start()
        this.subscription?.dispose()
        this.subscribed = true
        val heads = Flux.merge(
            // get the current block, not just wait for the next update
            getLatestBlock(api),
            listenNewHeads(),
        )
        this.subscription = super.follow(heads)
    }

    override fun onNoHeadUpdates() {
        noHeadUpdatesSink.tryEmitNext(true)
    }

    override fun onSyncingNode(isSyncing: Boolean) {
        if (isSyncing && !this.isSyncing) {
            cancelSub()
        }
        this.isSyncing = isSyncing
    }

    fun listenNewHeads(): Flux<BlockContainer> {
        return subscribe()
            .map {
                chainSpecific.parseHeader(it, "unknown")
            }
            .timeout(Duration.ofSeconds(60), Mono.error(RuntimeException("No response from subscribe to newHeads")))
            .onErrorResume {
                log.error("Error getting heads for $upstreamId - ${it.message}")
                subscribed = false
                unsubscribe()
            }
    }

    override fun stop() {
        super.stop()
        cancelSub()
        noHeadUpdatesSink.tryEmitComplete()
    }

    override fun headLiveness(): Flux<Boolean> = headLivenessSink.asFlux()

    private fun unsubscribe(): Mono<BlockContainer> {
        return wsSubscriptions.unsubscribe(chainSpecific.unsubscribeNewHeadsRequest(subscriptionId.get()).copy(id = ids.getAndIncrement()))
            .flatMap { it.requireResult() }
            .doOnNext { log.warn("{} has just unsubscribed from newHeads", upstreamId) }
            .onErrorResume {
                log.error("{} couldn't unsubscribe from newHeads", upstreamId, it)
                Mono.empty()
            }
            .then(Mono.empty())
    }

    private val ids = AtomicInteger(1)

    private fun subscribe(): Flux<ByteArray> {
        return try {
            wsSubscriptions.subscribe(chainSpecific.listenNewHeadsRequest().copy(id = ids.getAndIncrement()))
                .also {
                    connectionId = it.connectionId
                    subscriptionId = it.subId
                    if (!connected) {
                        connected = true
                    }
                }.data
        } catch (e: Exception) {
            Flux.error(e)
        }
    }

    private fun registerHeadResubscribeFlux() {
        val connectionStates = wsSubscriptions.connectionInfoFlux()
            .map {
                if (it.connectionId == connectionId && it.connectionState == WsConnection.ConnectionState.DISCONNECTED) {
                    headLivenessSink.emitNext(false) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                    subscribed = false
                    connected = false
                    connectionId = null
                } else if (it.connectionState == WsConnection.ConnectionState.CONNECTED) {
                    connected = true
                    return@map true
                }
                return@map false
            }

        Flux.merge(
            noHeadUpdatesSink.asFlux(),
            connectionStates,
        ).subscribeOn(wsConnectionResubscribeScheduler)
            .filter { it && !subscribed && connected && !isSyncing }
            .subscribe {
                log.warn("Restart ws head, upstreamId: $upstreamId")
                start()
            }
    }

    private fun cancelSub() {
        subscription?.dispose()
        subscription = null
        subscribed = false
    }
}
