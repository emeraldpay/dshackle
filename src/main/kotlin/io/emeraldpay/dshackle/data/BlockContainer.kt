/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.data

import io.emeraldpay.dshackle.Global
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import java.math.BigInteger
import java.time.Instant

class BlockContainer(
    val height: Long,
    val hash: BlockId,
    val parentHash: BlockId?,
    val difficulty: BigInteger,
    val timestamp: Instant,
    val includesFullTransactions: Boolean,
    json: ByteArray?,
    val parsed: Any?,
    val transactions: List<TxId> = emptyList(),
) : SourceContainer(json, parsed) {
    constructor(height: Long, hash: BlockId, difficulty: BigInteger, timestamp: Instant) :
        this(height, hash, null, difficulty, timestamp, false, null, null)

    constructor(height: Long, hash: BlockId, difficulty: BigInteger, timestamp: Instant, transactions: List<TxId>) :
        this(height, hash, null, difficulty, timestamp, false, null, null, transactions)

    companion object {
        @JvmStatic
        fun from(
            block: BlockJson<*>,
            raw: ByteArray,
        ): BlockContainer {
            val hasTransactions = (block.transactions?.filterIsInstance<TransactionJson>()?.count() ?: 0) > 0
            return BlockContainer(
                block.number,
                BlockId.from(block),
                block.parentHash?.let(BlockId.Companion::from),
                block.totalDifficulty ?: BigInteger.ZERO,
                block.timestamp,
                hasTransactions,
                raw,
                block,
                block.transactions?.map { TxId.from(it.hash) } ?: emptyList(),
            )
        }

        @JvmStatic
        fun from(block: BlockJson<*>): BlockContainer = from(block, Global.objectMapper.writeValueAsBytes(block))

        @JvmStatic
        fun fromEthereumJson(raw: ByteArray): BlockContainer {
            val block = Global.objectMapper.readValue(raw, BlockJson::class.java)
            return from(block, raw)
        }
    }

    override fun toString(): String = "Block $height = $hash"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as BlockContainer

        if (height != other.height) return false
        if (hash != other.hash) return false
        if (difficulty != other.difficulty) return false
        if (timestamp != other.timestamp) return false
        if (includesFullTransactions != other.includesFullTransactions) return false
        if (transactions != other.transactions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + height.hashCode()
        result = 31 * result + hash.hashCode()
        return result
    }
}
