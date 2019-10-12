/**
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
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.ReactorBatch
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
    private val api: DirectEthereumApi,
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
                    api.rpcClient
                            .execute(Commands.eth().blockNumber)
                            .subscribeOn(scheduler)
                            .timeout(Defaults.timeout, Mono.error(Exception("Block number not received")))
                }
                .flatMap {
                    api.rpcClient
                            .execute(Commands.eth().getBlock(it))
                            .subscribeOn(scheduler)
                            .timeout(Defaults.timeout, Mono.error(Exception("Block data not received")))
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