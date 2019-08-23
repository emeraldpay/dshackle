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

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

class EthereumHeadMerge(
        fluxes: Iterable<Publisher<BlockJson<TransactionId>>>
): EthereumHead, Lifecycle {

    private val log = LoggerFactory.getLogger(EthereumHeadMerge::class.java)
    private val flux: Flux<BlockJson<TransactionId>>
    private val head = AtomicReference<BlockJson<TransactionId>>(null)
    private var subscription: Disposable? = null

    init {
        flux = Flux.merge(fluxes)
                .distinctUntilChanged {
                    it.hash
                }
                .filter {
                    val curr = head.get()
                    curr == null || curr.totalDifficulty < it.totalDifficulty
                }
                .publish()
                .autoConnect()
    }

    override fun isRunning(): Boolean {
        return subscription != null
    }

    override fun start() {
        subscription = Flux.from(flux).subscribe {
            head.set(it)
        }
    }

    override fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.merge(
                Mono.justOrEmpty(head.get()),
                Flux.from(this.flux)
        ).onBackpressureLatest()
    }

    override fun stop() {
        subscription?.dispose()
    }

}