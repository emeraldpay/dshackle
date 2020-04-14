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
package io.emeraldpay.dshackle.data

import com.fasterxml.jackson.databind.ObjectMapper
import io.infinitape.etherjar.rpc.json.TransactionJson

class TxContainer(
        val height: Long,
        val hash: TxId,
        val blockId: BlockId?,
        json: ByteArray?
) : SourceContainer(json) {

    companion object {
        @JvmStatic
        fun from(tx: TransactionJson, objectMapper: ObjectMapper): TxContainer {
            return TxContainer(
                    tx.blockNumber,
                    TxId.from(tx.hash),
                    BlockId.from(tx.blockHash),
                    objectMapper.writeValueAsBytes(tx)
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