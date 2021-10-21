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

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.Hex32
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.rpc.json.HexDataSerializer

data class LogMessage(
    @get:JsonSerialize(using = HexDataSerializer::class)
    val address: Address,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val blockHash: BlockHash,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val blockNumber: Long,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val data: HexData,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val logIndex: Long,
    @get:JsonSerialize(contentUsing = HexDataSerializer::class)
    val topics: List<Hex32>,
    @get:JsonSerialize(using = HexDataSerializer::class)
    val transactionHash: TransactionId,
    @get:JsonSerialize(using = NumberAsHexSerializer::class)
    val transactionIndex: Long,
    val removed: Boolean
)
