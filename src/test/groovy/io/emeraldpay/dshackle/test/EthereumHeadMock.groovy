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
package io.emeraldpay.dshackle.test

import io.emeraldpay.dshackle.upstream.ethereum.EthereumHead
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor

class EthereumHeadMock implements EthereumHead {

    private TopicProcessor<BlockJson<TransactionId>> bus = TopicProcessor.create()
    private BlockJson<TransactionId> latest

    void nextBlock(BlockJson<TransactionId> block) {
        assert block != null
        latest = block
        bus.onNext(block)
    }

    @Override
    Flux<BlockJson<TransactionId>> getFlux() {
        return Flux.concat(Mono.justOrEmpty(latest), bus).distinctUntilChanged()
    }
}
