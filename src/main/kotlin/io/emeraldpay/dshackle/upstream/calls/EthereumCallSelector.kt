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
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.Collections
import java.util.Objects

/**
 * Get a matcher based on a criteria provided with a RPC request. I.e. when the client requests data for "latest", or "0x19f816" block.
 * The implementation is specific for Ethereum.
 */
class EthereumCallSelector(
    private val heightReader: Reader<BlockId, Long>
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumCallSelector::class.java)

        // ref https://eth.wiki/json-rpc/API#the-default-block-parameter
        private val TAG_METHODS = listOf(
            "eth_getBalance",
            "eth_getCode",
            "eth_getTransactionCount",
            // no "eth_getStorageAt" because it has different structure, and therefore separate logic
            "eth_call"
        ).sorted()
    }

    private val objectMapper = Global.objectMapper

    /**
     * @param method JSON RPC name
     * @param params JSON-encoded list of parameters for the method
     */
    fun getMatcher(method: String, params: String, head: Head): Mono<Selector.Matcher> {
        if (Collections.binarySearch(TAG_METHODS, method) >= 0) {
            return blockTagSelector(params, 1, head)
        } else if (method == "eth_getStorageAt") {
            return blockTagSelector(params, 2, head)
        } else if (method in DefaultEthereumMethods.withFilterIdMethods) {
            return sameUpstreamMatcher(params)
        }
        return Mono.empty()
    }

    private fun sameUpstreamMatcher(params: String): Mono<Selector.Matcher> {
        val list = objectMapper.readerFor(Any::class.java).readValues<Any>(params).readAll()
        if (list.isEmpty()) {
            return Mono.empty()
        }
        val filterId = list[0].toString()
        if (filterId.length < 4) {
            return Mono.just(Selector.SameNodeMatcher(0.toByte()))
        }
        val hashHex = filterId.substring(filterId.length - 2)
        val nodeId = hashHex.toInt(16)
        return Mono.just(Selector.SameNodeMatcher(nodeId.toByte()))
    }

    private fun blockTagSelector(params: String, pos: Int, head: Head): Mono<Selector.Matcher> {
        val list = objectMapper.readerFor(Any::class.java).readValues<Any>(params).readAll()
        if (list.size < pos + 1) {
            log.debug("Tag is not specified. Ignoring")
            return Mono.empty()
        }
        // integer block number, a string "latest", "earliest" or "pending", or an object with block reference
        val minHeight: Long? = when (val tag = Objects.toString(list[pos])) {
            "latest" -> head.getCurrentHeight()
            // for earliest it doesn't nothing, we expect to have 0 block
            "earliest" -> 0L
            else -> if (tag.startsWith("0x")) {
                return if (tag.length == 66) { // 32-byte hash is represented as 0x + 64 characters
                    blockByHash(tag, head)
                } else {
                    blockByHeight(tag)
                }
            } else if (tag.startsWith("{") && list[pos] is Map<*, *>) {
                // see https://eips.ethereum.org/EIPS/eip-1898
                val obj = list[pos] as Map<*, *>
                when {
                    obj.containsKey("blockNumber") -> {
                        return blockByHeight(obj["blockNumber"].toString())
                    }
                    obj.containsKey("blockHash") -> {
                        return blockByHash(obj["blockHash"].toString(), head)
                    }
                    else -> null
                }
            } else {
                log.debug("Invalid tag: $tag")
                null
            }
        }
        return if (minHeight != null && minHeight >= 0) {
            Mono.just(Selector.HeightMatcher(minHeight))
        } else {
            Mono.empty()
        }
    }

    private fun blockByHeight(blockNumber: String): Mono<Selector.Matcher> {
        return try {
            Mono.just(Selector.HeightMatcher(HexQuantity.from(blockNumber).value.longValueExact()))
        } catch (t: Throwable) {
            log.warn("Invalid blockNumber: $blockNumber")
            Mono.empty<Selector.Matcher>()
        }
    }

    private fun blockByHash(blockHash: String, head: Head): Mono<Selector.Matcher> {
        return try {
            val blockId = BlockId.from(blockHash)
            heightReader.read(blockId)
                .switchIfEmpty(Mono.justOrEmpty(head.getCurrentHeight()))
                .map { Selector.HeightMatcher(it) }
        } catch (t: Throwable) {
            log.warn("Invalid blockHash: $blockHash")
            Mono.empty<Selector.Matcher>()
        }
    }
}
