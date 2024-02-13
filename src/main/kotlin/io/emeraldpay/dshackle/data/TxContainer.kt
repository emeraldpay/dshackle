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
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJsonSnapshot

class TxContainer(
    val height: Long?,
    val hash: TxId,
    val blockId: BlockId?,
    json: ByteArray?,
    parsed: Any? = null,
) : SourceContainer(json, parsed) {

    companion object {
        @JvmStatic
        fun from(raw: ByteArray): TxContainer {
            val tx = Global.objectMapper.readValue(raw, TransactionJson::class.java)
            return from(tx, raw)
        }

        @JvmStatic
        fun from(tx: TransactionJson): TxContainer {
            return from(tx, Global.objectMapper.writeValueAsBytes(tx))
        }

        fun from(tx: TransactionJson, raw: ByteArray): TxContainer {
            return TxContainer(
                tx.blockNumber,
                TxId.from(tx.hash),
                tx.blockHash?.let { BlockId.from(it) },
                raw,
                tx,
            )
        }

        fun from(tx: TransactionJsonSnapshot, raw: ByteArray): TxContainer {
            return TxContainer(
                tx.blockNumber,
                TxId.from(tx.hash),
                tx.blockHash?.let { BlockId.from(it) },
                raw,
                tx,
            )
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as TxContainer

        if (height != other.height) return false
        if (hash != other.hash) return false
        if (blockId != other.blockId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + height.hashCode()
        result = 31 * result + hash.hashCode()
        return result
    }
}
