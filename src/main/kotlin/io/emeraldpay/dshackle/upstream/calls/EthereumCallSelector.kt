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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.infinitape.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Get a matcher based on a criteria provided with a RPC request. I.e. when the client requests data for "latest", or "0x19f816" block.
 * The implementation is specific for Ethereum.
 */
class EthereumCallSelector {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumCallSelector::class.java)
        // ref https://eth.wiki/json-rpc/API#the-default-block-parameter
        private val TAG_METHODS = listOf(
                "eth_getBalance",
                "eth_getCode",
                "eth_getTransactionCount",
                // no "eth_getStorageAt" because it's has different structure, and therefore separate logic
                "eth_call"
        ).sorted()
    }

    private val objectMapper = Global.objectMapper

    /**
     * @param method JSON RPC name
     * @param params JSON-encoded list of parameters for the method
     */
    fun getMatcher(method: String, params: String, head: Head): Selector.Matcher? {
        if (Collections.binarySearch(TAG_METHODS, method) >= 0) {
            return blockTagSelector(params, 1, head)
        } else if (method == "eth_getStorageAt") {
            return blockTagSelector(params, 2, head)
        }
        return null
    }

    private fun blockTagSelector(params: String, pos: Int, head: Head): Selector.Matcher? {
        val list = objectMapper.readerFor(Any::class.java).readValues<Any>(params).readAll()
        if (list.size < pos + 1) {
            log.debug("Tag is not specified. Ignoring")
            return null
        }
        // integer block number, or the string "latest", "earliest" or "pending"
        val minHeight = when (val tag = list[pos].toString()) {
            "latest" -> head.getCurrentHeight() ?: 0
            // for earliest it doesn't nothing, we expect to have 0 block
            "earliest" -> 0L
            else -> if (tag.startsWith("0x")) {
                    try {
                        HexQuantity.from(tag).value.toLong()
                    } catch (t: Throwable) {
                        log.debug("Invalid tag: $tag. ${t.javaClass}: ${t.message}")
                        0L
                    }
                } else {
                    log.debug("Invalid tag: $tag")
                    0L
                }
        }
        return if (minHeight > 0) {
            Selector.HeightMatcher(minHeight)
        } else {
            null
        }
    }

}