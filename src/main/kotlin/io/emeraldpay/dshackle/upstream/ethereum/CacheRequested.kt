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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.upstream.RequestPostprocessor
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import org.slf4j.LoggerFactory

class CacheRequested(
        private val caches: Caches
) : RequestPostprocessor {

    companion object {
        private val log = LoggerFactory.getLogger(CacheRequested::class.java)
    }

    override fun onReceive(method: String, params: List<Any>, json: ByteArray) {
        try {
            if (method == "eth_getTransactionReceipt") {
                cacheTxReceipt(params, json)
            }
        } catch (e: Throwable) {
            log.warn("Failed to cache result", e)
        }
    }

    fun cacheTxReceipt(params: List<Any>, json: ByteArray) {
        if (params.size != 1) {
            return
        }
        // note: json could be a `null` value
        val parsed = Global.objectMapper.readValue(json, TransactionReceiptJson::class.java) ?: return
        val value = DefaultContainer<TransactionReceiptJson>(
                TxId.from(parsed.transactionHash),
                BlockId.from(parsed.blockHash),
                parsed.blockNumber,
                json,
                parsed
        )
        caches.cacheReceipt(Caches.Tag.REQUESTED, value)
    }

}