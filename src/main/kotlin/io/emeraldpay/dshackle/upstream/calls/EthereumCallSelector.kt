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
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.etherjar.hex.HexQuantity
import org.bouncycastle.util.encoders.DecoderException
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.Objects

/**
 * Get a matcher based on a criteria provided with a RPC request. I.e. when the client requests data for "latest", or "0x19f816" block.
 * The implementation is specific for Ethereum.
 */
class EthereumCallSelector(
    private val caches: Caches
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumCallSelector::class.java)

        // ref https://eth.wiki/json-rpc/API#the-default-block-parameter
        private val TAG_METHODS = setOf(
            "eth_getBalance",
            "eth_getCode",
            "eth_getProof",
            "eth_getTransactionCount",
            // no "eth_getStorageAt" because it has different structure, and therefore separate logic
            "eth_call",
            "eth_getRootHash",
            "bor_getRootHash"
        )

        private val FILTER_OBJECT_METHODS = setOf(
            "eth_newFilter", "eth_getLogs"
        )

        private val GET_BY_HASH_OR_NUMBER_METHODS = setOf(
            "eth_getBlockByHash", "eth_getBlockByNumber", "bor_getSignersAtHash",
            "eth_getTransactionByBlockHashAndIndex", "eth_getTransactionByBlockNumberAndIndex",
            "eth_getBlockTransactionCountByNumber", "bor_getAuthor", "eth_getUncleCountByBlockNumber",
            "eth_getUncleByBlockNumberAndIndex"
        )
    }

    private val objectMapper = Global.objectMapper

    /**
     * @param method JSON RPC name
     * @param params JSON-encoded list of parameters for the method
     */
    fun getMatcher(method: String, params: String, head: Head, passthrough: Boolean): Mono<Selector.Matcher> {
        if (method in DefaultEthereumMethods.withFilterIdMethods) {
            return sameUpstreamMatcher(params)
        } else if (!passthrough) { // passthrough indicates we should match only labels
            when (method) {
                in TAG_METHODS -> {
                    return blockTagSelector(params, 1, null, head)
                }
                "eth_getStorageAt" -> {
                    return blockTagSelector(params, 2, null, head)
                }
                in GET_BY_HASH_OR_NUMBER_METHODS -> {
                    return blockTagSelector(params, 0, null, head)
                }
                in FILTER_OBJECT_METHODS -> {
                    return blockTagSelector(params, 0, "toBlock", head)
                }
            }
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

    private fun blockTagSelector(params: String, pos: Int, paramName: String?, head: Head): Mono<Selector.Matcher> {
        val list = objectMapper.readerFor(Any::class.java).readValues<Any>(params).readAll()
        if (list.size < pos + 1) {
            log.debug("Tag is not specified. Ignoring")
            return Mono.empty()
        }

        // integer block number, a string "latest", "earliest" or "pending", or an object with block reference
        val blockTag = Objects.toString(list[pos])

        return if (blockTag.startsWith("{") && list[pos] is Map<*, *>) {
            val obj = list[pos] as Map<*, *>
            when {
                paramName != null -> {
                    return if (obj.containsKey(paramName)) {
                        blockSelectorByTag(obj[paramName].toString(), head)
                    } else {
                        Mono.empty()
                    }
                }
                obj.containsKey("blockNumber") -> {
                    return blockSelectorByTag(obj["blockNumber"].toString(), head)
                }
                obj.containsKey("blockHash") -> {
                    return blockSelectorByTag(obj["blockHash"].toString(), head)
                }
                else -> {
                    log.debug("Tag is not found. Ignoring")
                    Mono.empty()
                }
            }
        } else {
            blockSelectorByTag(blockTag, head)
        }
    }

    private fun blockSelectorByTag(tag: String, head: Head): Mono<Selector.Matcher> {
        val minHeight: Long? = when (tag) {
            "latest" -> head.getCurrentHeight()
            "pending" -> null
            "earliest" -> 0L // for earliest it doesn't nothing, we expect to have 0 block
            else -> if (tag.startsWith("0x") || tag.toLongOrNull() != null) {
                return if (tag.length == 66) { // 32-byte hash is represented as 0x + 64 characters
                    blockByHash(tag)
                } else {
                    blockByHeight(tag)
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

    private fun blockByHash(blockHash: String): Mono<Selector.Matcher> {
        return try {
            caches.getLastHeightByHash()
                .read(BlockId.from(blockHash))
                .onErrorResume { Mono.empty() }
                .map { Selector.HeightMatcher(it) }
        } catch (e: DecoderException) {
            log.warn("Invalid blockHash: $blockHash")
            Mono.empty()
        }
    }

    private fun blockByHeight(blockNumber: String): Mono<Selector.Matcher> {
        return try {
            val height =
                if (blockNumber.startsWith("0x")) {
                    HexQuantity.from(blockNumber).value.longValueExact()
                } else {
                    blockNumber.toLong()
                }
            Mono.just(Selector.HeightMatcher(height))
        } catch (t: Throwable) {
            log.warn("Invalid blockNumber: $blockNumber")
            Mono.empty()
        }
    }
}
