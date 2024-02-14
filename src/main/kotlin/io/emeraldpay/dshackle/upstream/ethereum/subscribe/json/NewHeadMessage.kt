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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.Bloom
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData
import io.emeraldpay.dshackle.upstream.ethereum.json.HexDataSerializer
import java.math.BigInteger
import java.time.Instant

/**
 * Common fields for newHeads event. IT's different from Block JSON and doesn't include many fields, most notable is
 * list of transactions.
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
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val difficulty: BigInteger?,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val gasLimit: Long?,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val gasUsed: Long,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val logsBloom: Bloom,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val miner: Address,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val baseFeePerGas: BigInteger?,

    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val extraData: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val mixHash: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val nonce: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val receiptsRoot: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val sha3Uncles: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val stateRoot: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val transactionsRoot: HexData?,
    @get:JsonSerialize(using = HexDataSerializer::class)
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val withdrawalsRoot: HexData?,

    @get:JsonIgnore
    override val upstreamId: String,

    // lists always empty
    val transactions: List<TransactionId> = emptyList(),
    val uncles: List<BlockHash> = emptyList(),
    val sealFields: List<BlockHash> = emptyList(),
) : HasUpstream
