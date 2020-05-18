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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.Commands
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors

class EthereumRpcHead(
        private val api: Reader<JsonRpcRequest, JsonRpcResponse>,
        private val interval: Duration = Duration.ofSeconds(10)
): DefaultEthereumHead(), Lifecycle {

    companion object {
        val scheduler = Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("ethereum-rpc-head")))
    }

    private val log = LoggerFactory.getLogger(EthereumRpcHead::class.java)

    private var refreshSubscription: Disposable? = null

    override fun start() {
        val base = Flux.interval(interval)
                .publishOn(scheduler)
                .flatMap {
                    api.read(JsonRpcRequest("eth_blockNumber", emptyList()))
                            .subscribeOn(scheduler)
                            .timeout(Defaults.timeout, Mono.error(Exception("Block number not received")))
                            .flatMap {
                                if (it.error != null) {
                                    Mono.error(it.error.asException())
                                } else {
                                    val value = it.getResultAsProcessedString()
                                    Mono.just(HexQuantity.from(value))
                                }
                            }
                }
                .flatMap {
                    //fetching by Block Height here, critical to use same upstream,
                    //different upstreams may have different blocks on the same height
                    api.read(JsonRpcRequest("eth_getBlockByNumber", listOf(it.toHex(), false)))
                            .subscribeOn(scheduler)
                            .timeout(Defaults.timeout, Mono.error(Exception("Block data not received")))
                }
                .map {
                    BlockContainer.from(it.getResult())
                }
                .onErrorContinue { err, _ ->
                    log.debug("RPC error ${err.message}")
                }
        refreshSubscription = super.follow(base)
    }

    override fun isRunning(): Boolean {
        return refreshSubscription != null
    }

    override fun stop() {
        refreshSubscription?.dispose()
        refreshSubscription = null
    }

}