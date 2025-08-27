/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import java.math.BigInteger
import java.time.Instant

class ExtractBlock {
    companion object {
        private val log = LoggerFactory.getLogger(ExtractBlock::class.java)

        @JvmStatic
        fun getHeight(data: Map<String, Any>): Long? {
            val height = data["height"] as Number? ?: return null
            return height.toLong()
        }

        @JvmStatic
        fun getTime(data: Map<String, Any>): Instant? {
            val time = data["time"] as Number? ?: return null
            return Instant.ofEpochMilli(time.toLong() * 1000)
        }

        @JvmStatic
        fun getDifficulty(data: Map<String, Any>): BigInteger? {
            val chainwork = data["chainwork"] as String? ?: return null
            return BigInteger(1, Hex.decodeHex(chainwork))
        }
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    @Suppress("UNCHECKED_CAST")
    fun extract(json: ByteArray): BlockContainer {
        val data = objectMapper.readValue(json, Map::class.java) as Map<String, Any>

        val hash = data["hash"] as String? ?: throw IllegalArgumentException("Block JSON has no hash")
        val previousBlockHas = data["previousblockhash"] as String? ?: throw IllegalArgumentException("Block JSON has no previousblockhash")
        val transactions = (data["tx"] as List<String>?)?.map(TxId.Companion::from) ?: emptyList()

        return BlockContainer(
            height = getHeight(data) ?: throw IllegalArgumentException("Block JSON has no height"),
            hash = BlockId.from(hash),
            BlockId.from(previousBlockHas),
            difficulty = getDifficulty(data) ?: throw IllegalArgumentException("Block JSON has no chainwork"),
            timestamp = getTime(data) ?: throw IllegalArgumentException("Block JSON has no time"),
            includesFullTransactions = false,
            json = json,
            parsed = data,
            transactions = transactions,
        )
    }
}
