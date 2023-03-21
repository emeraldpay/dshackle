/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Head
import org.jetbrains.annotations.NotNull
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks

class EthereumHeadMock implements Head {

    private Sinks.Many<BlockContainer> bus = Sinks.many().multicast().onBackpressureBuffer()
    private Publisher<BlockContainer> predefined = null
    private BlockContainer latest
    private List<Runnable> handlers = []

    void nextBlock(BlockContainer block) {
        handlers.forEach {
            it.run()
        }
        assert block != null
        println("New block: ${block.height} / ${block.hash}")
        latest = block
        bus.tryEmitNext(block)
    }

    void setPredefined(Publisher<BlockContainer> predefined) {
        this.predefined = Flux.from(predefined)
                .publish()
                .refCount(1)
                .doOnNext { latest = it }
        // keep the current block as latest, because getFlux is also used to get the current height
    }

    @Override
    Flux<BlockContainer> getFlux() {
        if (predefined != null) {
            return Flux.concat(Mono.justOrEmpty(latest), Flux.from(predefined))
        } else {
            return Flux.concat(Mono.justOrEmpty(latest), bus.asFlux()).distinctUntilChanged()
        }
    }

    @Override
    void onBeforeBlock(@NotNull Runnable handler) {
        handlers.add(handler)
    }

    @Override
    Long getCurrentHeight() {
        return latest?.height
    }

    @Override
    void start() {

    }

    @Override
    void stop() {

    }

    @Override
    void onNoHeadUpdates() {

    }
}
