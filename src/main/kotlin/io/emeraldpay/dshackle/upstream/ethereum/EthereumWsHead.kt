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

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.retry.Repeat
import java.time.Duration

class EthereumWsHead(
    upstreamId: String,
    forkChoice: ForkChoice,
    blockValidator: BlockValidator,
    private val api: JsonRpcReader,
    private val wsSubscriptions: WsSubscriptions,
    private val skipEnhance: Boolean,
    private val wsConnectionResubscribeScheduler: Scheduler,
    headScheduler: Scheduler,
) : DefaultEthereumHead(upstreamId, forkChoice, blockValidator, headScheduler), Lifecycle {

    private var connectionId: String? = null
    private var subscribed = false
    private var connected = false

    private var subscription: Disposable? = null
    private val noHeadUpdatesSink = Sinks.many().multicast().directBestEffort<Boolean>()

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
            listenNewHeads()
        )
        this.subscription = super.follow(heads)
    }

    override fun onNoHeadUpdates() {
        noHeadUpdatesSink.tryEmitNext(true)
    }

    fun listenNewHeads(): Flux<BlockContainer> {
        return subscribe()
            .map {
                Global.objectMapper.readValue(it, BlockJson::class.java) as BlockJson<TransactionRefJson>
            }
            .flatMap { block ->
                // newHeads returns incomplete blocks, i.e. without some fields and without transaction hashes,
                // so we need to fetch the full block data
                if (!skipEnhance && (
                    block.difficulty == null ||
                        block.transactions == null ||
                        block.transactions.isEmpty() ||
                        block.totalDifficulty == null
                    )
                ) {
                    enhanceRealBlock(block)
                } else {
                    Mono.just(BlockContainer.from(block))
                }
            }
            .timeout(Duration.ofSeconds(60), Mono.error(RuntimeException("No response from subscribe to newHeads")))
            .onErrorResume {
                subscribed = false
                Mono.empty()
            }
    }

    fun enhanceRealBlock(block: BlockJson<TransactionRefJson>): Mono<BlockContainer> {
        return Mono.just(block.hash)
            .flatMap { hash ->
                api.read(JsonRpcRequest("eth_getBlockByHash", listOf(hash.toHex(), false)))
                    .flatMap { resp ->
                        if (resp.isNull()) {
                            Mono.error(SilentException("Received null for block $hash"))
                        } else {
                            Mono.just(resp)
                        }
                    }
                    .flatMap(JsonRpcResponse::requireResult)
                    .map { BlockContainer.fromEthereumJson(it, upstreamId) }
                    .subscribeOn(Schedulers.boundedElastic())
                    .timeout(Defaults.timeoutInternal, Mono.empty())
            }.repeatWhenEmpty { n ->
                Repeat.times<Any>(5)
                    .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(500))
                    .apply(n)
            }
            .timeout(Defaults.timeout, Mono.empty())
            .onErrorResume { Mono.empty() }
    }

    override fun stop() {
        super.stop()
        subscription?.dispose()
        subscription = null
        noHeadUpdatesSink.tryEmitComplete()
    }

    private fun subscribe(): Flux<ByteArray> {
        return try {
            wsSubscriptions.subscribe("newHeads")
                .also {
                    connectionId = it.connectionId
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
            .filter { it && !subscribed && connected }
            .subscribe {
                log.warn("Restart ws head, upstreamId: $upstreamId")
                start()
            }
    }
}
