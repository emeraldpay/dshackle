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
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Head
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.time.Duration

class EthereumHeadLagObserver(
        master: Head,
        followers: Collection<Upstream<EthereumApi>>
) : HeadLagObserver<EthereumApi>(master, followers) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumHeadLagObserver::class.java)
    }

    override fun getCurrentBlocks(up: Upstream<EthereumApi>): Flux<BlockContainer> {
        val head = up.getHead()
        return head.getFlux().take(Duration.ofSeconds(1))
    }

    override fun extractDistance(top: BlockContainer, curr: BlockContainer): Long {
        return when {
            curr.height > top.height -> if (curr.difficulty >= top.difficulty) 0 else forkDistance(top, curr)
            curr.height == top.height -> if (curr.difficulty == top.difficulty) 0 else forkDistance(top, curr)
            else -> top.height - curr.height
        }
    }
}