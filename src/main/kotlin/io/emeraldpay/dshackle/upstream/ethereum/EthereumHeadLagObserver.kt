/**
 * Copyright (c) 2020 ETCDEV GmbH
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

import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.time.Duration

class EthereumHeadLagObserver(
        master: EthereumHead,
        followers: Collection<Upstream<EthereumApi, BlockJson<TransactionRefJson>>>
) : HeadLagObserver<EthereumApi, BlockJson<TransactionRefJson>>(master, followers) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumHeadLagObserver::class.java)
    }

    override fun getCurrentBlocks(up: Upstream<EthereumApi, BlockJson<TransactionRefJson>>): Flux<BlockJson<TransactionRefJson>> {
        val head = up.getHead()
        return Flux.from(head.getFlux()).take(Duration.ofSeconds(1))
    }

    override fun extractDistance(top: BlockJson<TransactionRefJson>, curr: BlockJson<TransactionRefJson>): Long {
        return when {
            curr.number > top.number -> if (curr.totalDifficulty >= top.totalDifficulty) 0 else forkDistance(top, curr)
            curr.number == top.number -> if (curr.totalDifficulty == top.totalDifficulty) 0 else forkDistance(top, curr)
            else -> top.number - curr.number
        }
    }
}