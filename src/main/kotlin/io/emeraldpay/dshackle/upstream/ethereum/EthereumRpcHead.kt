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
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

class EthereumRpcHead(
    private val api: DirectEthereumApi,
    private val interval: Duration = Duration.ofSeconds(10)
): DefaultEthereumHead(), Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumRpcHead::class.java)

    private var refreshSubscription: Disposable? = null

    override fun start() {
        val base = Flux.interval(interval)
                .flatMap {
                    val batch = Batch()
                    val f = batch.add(Commands.eth().blockNumber)
                    api.rpcClient.execute(batch)
                    Mono.fromCompletionStage(f).timeout(Defaults.timeout, Mono.empty())
                }
                .flatMap {
                    val batch = Batch()
                    val f = batch.add(Commands.eth().getBlock(it))
                    api.rpcClient.execute(batch)
                    Mono.fromCompletionStage(f).timeout(Defaults.timeout, Mono.empty())
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