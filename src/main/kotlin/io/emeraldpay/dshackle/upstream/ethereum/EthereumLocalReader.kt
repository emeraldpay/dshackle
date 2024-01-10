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

import io.emeraldpay.dshackle.Global.Companion.nullValue
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.LogsOracle
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.hex.HexQuantity
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.math.BigInteger

/**
 * Reader for JSON RPC requests. Verifies if the method is allowed, transforms if necessary, and calls EthereumReader for data.
 * It provides data only if it's available through the router (cached, head, etc).
 * If data is not available locally then it returns `empty`; at this case the caller should call the remote node for actual data.
 *
 * @see EthereumCachingReader
 */
class EthereumLocalReader(
    private val reader: EthereumCachingReader,
    private val methods: CallMethods,
    private val head: Head,
    private val logsOracle: LogsOracle?,
) : JsonRpcReader {

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        if (methods.isHardcoded(key.method)) {
            return Mono.just(methods.executeHardcoded(key.method))
                .map { JsonRpcResponse(it, null) }
        }
        if (!methods.isCallable(key.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        if (key.nonce != null) {
            // we do not want to serve any requests (except hardcoded) that have nonces from cache
            return Mono.empty()
        }
        val common = commonRequests(key)
            ?.switchIfEmpty { Mono.just(nullValue to null) }
        if (common != null) {
            return common.map { JsonRpcResponse(it.first, null, it.second) }
        }
        return Mono.empty()
    }

    /**
     * Prepare RpcCall with data types specific for that particular requests. In general it may return a call that just
     * parses JSON into Map. But the purpose of further processing and caching for some of the requests we want
     * to have actual data types.
     */
    fun commonRequests(key: JsonRpcRequest): Mono<Pair<ByteArray, String?>>? {
        val method = key.method
        val params = key.params
        return when {
            method == "eth_getTransactionByHash" -> {
                if (params.size != 1) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 1 parameter")
                }
                val hash: TxId
                try {
                    hash = TxId.from(params[0].toString())
                } catch (e: IllegalArgumentException) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be transaction id")
                }
                reader.txByHashAsCont()
                    .read(hash)
                    .map { it.data.json!! to it.upstreamId }
            }
            method == "eth_getBlockByHash" -> {
                if (params.size != 2) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 2 parameters")
                }
                val hash: BlockId
                try {
                    hash = BlockId.from(params[0].toString())
                } catch (e: IllegalArgumentException) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be block hash")
                }
                val withTx = params[1].toString().toBoolean()
                if (withTx) {
                    null
                } else {
                    reader.blocksByIdAsCont().read(hash).map { it.data.json!! to it.upstreamId }
                }
            }
            method == "eth_getBlockByNumber" -> {
                getBlockByNumber(params)
            }
            method == "eth_getTransactionReceipt" -> {
                if (params.size != 1) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 1 parameter")
                }
                val hash: TxId
                try {
                    hash = TxId.from(params[0].toString())
                } catch (e: IllegalArgumentException) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be transaction id")
                }
                reader.receipts()
                    .read(hash)
                    .map { it.data to it.upstreamId }
            }
            method == "drpc_getLogsEstimate" -> {
                getLogsEstimate(params)
            }
            else -> null
        }
    }

    fun getBlockByNumber(params: List<Any?>): Mono<Pair<ByteArray, String?>>? {
        if (params.size != 2 || params[0] == null || params[1] == null) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 2 parameters")
        }
        val number: Long
        val withTx = params[1].toString().toBoolean()
        if (withTx) {
            // with Tx request much more efficient in remote call
            return null
        }
        try {
            val blockRef = params[0].toString()
            when {
                blockRef.startsWith("0x") -> {
                    val quantity = HexQuantity.from(blockRef) ?: throw IllegalArgumentException()
                    number = quantity.value.let {
                        if (it < BigInteger.valueOf(Long.MAX_VALUE) && it >= BigInteger.ZERO) {
                            it.toLong()
                        } else {
                            throw IllegalArgumentException()
                        }
                    }
                }
                blockRef == "latest" -> {
                    number = head.getCurrentHeight() ?: return null
                }
                blockRef == "earliest" -> {
                    number = 0
                }
                blockRef == "finalized" || blockRef == "safe" || blockRef == "pending" -> {
                    return null
                }
                else -> {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Block number is invalid")
                }
            }
        } catch (e: IllegalArgumentException) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be a block number")
        }

        return reader.blocksByHeightAsCont()
            .read(number).map { it.data.json!! to it.upstreamId }
    }

    fun getLogsEstimate(params: List<Any?>): Mono<Pair<ByteArray, String?>>? {
        if (logsOracle == null) {
            throw NotImplementedError()
        }
        if (params.size != 1 || params[0] == null) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 1 parameters")
        }

        val req = params[0] as LinkedHashMap<String, Any?>

        val limit = try { req.get("limit") as Integer? } catch (_: IllegalArgumentException) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Invalid 'limit' parameter")
        }

        val fromBlock = try { parseBlockRef(req.get("fromBlock") as String?) } catch (_: IllegalArgumentException) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Invalid 'fromBlock' parameter")
        }
        val toBlock = try { parseBlockRef(req.get("toBlock") as String?) } catch (_: IllegalArgumentException) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Invalid 'toBlock' parameter")
        }
        val address: List<String> = try {
            val it = req.get("address") ?: listOf<String>()

            if (it is String) {
                listOf<String>(it)
            } else {
                it as List<String>
            }
        } catch (_: Exception) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Invalid 'address' parameter")
        }
        val topics: List<List<String>> = try {
            val tpcs = req.get("topics")?.let { it as List<Any> } ?: listOf<Any>()
            if (tpcs.size > 4) {
                throw IllegalArgumentException()
            }

            tpcs.map {
                if (it is String) {
                    listOf<String>(it)
                } else {
                    it as List<String>
                }
            }
        } catch (_: Exception) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Invalid 'topics' parameter")
        }

        return logsOracle.estimate(limit?.toLong() ?: null, fromBlock, toBlock, address, topics)
            .map { it.toByteArray() to null }
    }

    private fun parseBlockRef(blockRef: String?): Long {
        when {
            blockRef == "earliest" -> {
                return 0
            }
            blockRef == null || blockRef == "finalized" || blockRef == "safe" || blockRef == "pending" || blockRef == "latest" -> {
                return head.getCurrentHeight() ?: throw Exception("couldn't get current head")
            }
            blockRef.startsWith("0x") -> {
                val quantity = HexQuantity.from(blockRef) ?: throw IllegalArgumentException()
                return quantity.value.let {
                    if (it < BigInteger.valueOf(Long.MAX_VALUE) && it >= BigInteger.ZERO) {
                        it.toLong()
                    } else {
                        throw IllegalArgumentException()
                    }
                }
            }
            else -> {
                throw IllegalArgumentException()
            }
        }
    }
}
