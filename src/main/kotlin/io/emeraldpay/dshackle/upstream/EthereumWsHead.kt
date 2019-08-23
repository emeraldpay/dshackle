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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

class EthereumWsHead(
        private val ws: EthereumWs
): EthereumHead, Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumWsHead::class.java)

    private var subscription: Disposable? = null
    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private var stream: Flux<BlockJson<TransactionId>>? = null

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return stream?.let {
            Flux.merge(
                Mono.justOrEmpty(head.get()),
                Flux.from(this.stream)
            ).onBackpressureLatest()
        } ?: Flux.error(Exception("Not started"))
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        val flux = ws.getFlux()
            .distinctUntilChanged { it.hash }
            .filter { block ->
                val curr = head.get()
                curr == null || curr.totalDifficulty < block.totalDifficulty
            }.share()

        this.subscription = flux.subscribe(head::set)
        this.stream = flux
    }

    override fun stop() {
        subscription?.dispose()
        subscription = null
    }

}