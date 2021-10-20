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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe.json

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.Bloom
import io.emeraldpay.etherjar.rpc.json.HexDataSerializer
import java.math.BigInteger
import java.time.Instant

/**
 * Common fields for newHeads event. IT's different from Block JSON and doesn't include many fields, most notable is
 * list of transactions. Also, our JSON doesn't include rarely used fields such as extraData, sha3uncles, stateRoot,
 * transactionRoot and some others.
 */
data class NewHeadMessage(
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val number: Long,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val hash: BlockHash,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val parentHash: BlockHash,
    @get:JsonSerialize(using = TimestampSerializer::class)
    val timestamp: Instant,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val difficulty: BigInteger,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val gasLimit: Long,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val gasUsed: Long,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val logsBloom: Bloom,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val miner: Address,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val baseFeePerGas: BigInteger?
)
