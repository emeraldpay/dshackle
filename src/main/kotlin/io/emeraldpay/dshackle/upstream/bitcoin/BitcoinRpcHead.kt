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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors

class BitcoinRpcHead(
    private val api: JsonRpcReader,
    private val extractBlock: ExtractBlock,
    private val interval: Duration = Duration.ofSeconds(15)
) : Head, AbstractHead(), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinRpcHead::class.java)
        val scheduler =
            Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("bitcoin-rpc-head")))
    }

    private var refreshSubscription: Disposable? = null

    override fun isRunning(): Boolean {
        return refreshSubscription != null
    }

    override fun start() {
        if (refreshSubscription != null) {
            log.warn("Called to start when running")
            return
        }
        val base = Flux.interval(interval)
            .publishOn(scheduler)
            .flatMap {
                api.read(JsonRpcRequest("getbestblockhash", emptyList()))
                    .flatMap(JsonRpcResponse::requireStringResult)
                    .timeout(Defaults.timeout, Mono.error(SilentException.Timeout("Best block hash is not received")))
            }
            .distinctUntilChanged()
            .flatMap { hash ->
                api.read(JsonRpcRequest("getblock", listOf(hash)))
                    .flatMap(JsonRpcResponse::requireResult)
                    .map(extractBlock::extract)
                    .timeout(Defaults.timeout, Mono.error(SilentException.Timeout("Block data is not received")))
            }
            .onErrorContinue { err, _ ->
                log.debug("RPC error ${err.message}")
            }
        refreshSubscription = super.follow(base)
    }

    override fun stop() {
        val copy = refreshSubscription
        refreshSubscription = null
        copy?.dispose()
    }
}
