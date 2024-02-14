/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.NewHeadMessage
import reactor.core.publisher.Flux

/**
 * Produces NewHead messages by transforming blocks received from Head
 * @see Head
 * @see NewHeadMessage
 */
class ProduceNewHeads(
    val head: Head,
) {

    fun start(): Flux<ByteArray> {
        return head.getFlux()
            .map {
                if (it.full || it.json == null) {
                    val block = it.toBlock()
                    val msg = NewHeadMessage(
                        block.number,
                        block.hash,
                        block.parentHash,
                        block.timestamp,
                        block.difficulty,
                        block.gasLimit,
                        block.gasUsed,
                        block.logsBloom,
                        block.miner,
                        block.baseFeePerGas?.amount,
                        block.extraData ?: HexData.empty(),
                        block.mixHash,
                        block.nonce,
                        block.receiptsRoot,
                        block.sha3Uncles,
                        block.stateRoot,
                        block.transactionsRoot,
                        block.withdrawalsRoot,
                        it.upstreamId,
                    )
                    Global.objectMapper.writeValueAsBytes(msg)
                } else {
                    it.json
                }
            }
    }
}
