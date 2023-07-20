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

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.commons.DurableFlux
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.monitoring.record.RequestRecord
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.retry.Repeat
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

class EthereumWsHead(
    private val blockchain: Chain,
    private val api: StandardRpcReader,
    private val wsSubscriptions: WsSubscriptions,
) : DefaultEthereumHead(blockchain), Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumWsHead::class.java)

    private val running = AtomicBoolean(false)
    private var subscription: Disposable? = null

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        running.set(true)
        this.subscription?.dispose()
        val heads = Flux.merge(
            // get the current block immediately, not just wait for the next update
            getLatestBlock(api),
            listNewHeadsWithRetry()
        )
        this.subscription = super.follow(heads)
    }

    /**
     * WebSocket connection may disconnect, and it should retry again subscribing to newHeads
     */
    fun listNewHeadsWithRetry(): Flux<BlockContainer> {
        return DurableFlux.newBuilder()
            .logTo(log)
            .controlWith(running)
            .using { listenNewHeads() }
            .build()
            .connect()
    }

    fun listenNewHeads(): Flux<BlockContainer> {
        return wsSubscriptions.subscribe("newHeads")
            .contextWrite(Global.monitoring.ingress.withBlockchain(blockchain))
            .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.INTERNAL))
            .map {
                Global.objectMapper.readValue(it, BlockJson::class.java) as BlockJson<TransactionRefJson>
            }
            .flatMap { block ->
                // newHeads returns incomplete blocks, i.e. without some fields and without transaction hashes,
                // so we need to fetch the full block data
                if (block.difficulty == null || block.transactions == null) {
                    enhanceRealBlock(block)
                } else {
                    Mono.just(BlockContainer.from(block))
                }
            }
            // provide an error once the source completes, so we can retry to request heads again
            .concatWith(Mono.error(SilentException.DataUnavailable("newHeads")))
    }

    fun enhanceRealBlock(block: BlockJson<TransactionRefJson>): Mono<BlockContainer> {
        return Mono.just(block.hash)
            .flatMap { hash ->
                val request = JsonRpcRequest("eth_getBlockByHash", listOf(hash.toHex(), false))
                api.read(request)
                    .flatMap { resp ->
                        if (resp.isNull) {
                            Mono.error(SilentException("Received null for block $hash"))
                        } else {
                            Mono.just(resp)
                        }
                    }
                    .flatMap(JsonRpcResponse::requireResult)
                    .map { BlockContainer.fromEthereumJson(it) }
                    .subscribeOn(Schedulers.boundedElastic())
                    .contextWrite(Global.monitoring.ingress.withBlockchain(blockchain))
                    .contextWrite(Global.monitoring.ingress.withRequest(request))
                    .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.INTERNAL))
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
        running.set(false)
        subscription?.dispose()
        subscription = null
    }
}
