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

import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.hex.HexQuantity
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.math.BigInteger

/**
 * Reader for JSON RPC requests. Verifies if the method is allowed, transforms if necessary, and calls EthereumReader for data.
 * It provides data only if it's available through the router (cached, head, etc).
 * If data is not available locally then it returns `empty`; at this case the caller should call the remote node for actual data.
 *
 * @see EthereumCachingReader
 */
class LocalCallRouter(
    private val reader: EthereumCachingReader,
    private val methods: CallMethods,
    private val head: Head
) : Reader<JsonRpcRequest, JsonRpcResponse> {

    companion object {
        private val log = LoggerFactory.getLogger(LocalCallRouter::class.java)
    }

    private val fullBlocksReader = EthereumFullBlocksReader(
        reader.blocksByIdAsCont(),
        reader.txByHashAsCont()
    )

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
        if (common != null) {
            return common.map { JsonRpcResponse(it, null) }
        }
        return Mono.empty()
    }

    /**
     * Prepare RpcCall with data types specific for that particular requests. In general it may return a call that just
     * parses JSON into Map. But the purpose of further processing and caching for some of the requests we want
     * to have actual data types.
     */
    fun commonRequests(key: JsonRpcRequest): Mono<ByteArray>? {
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
                reader.txByHashAsCont().read(hash).map { it.json!! }
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
                    fullBlocksReader.read(hash).map { it.json!! }
                } else {
                    reader.blocksByIdAsCont().read(hash).map { it.json!! }
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
                reader.receipts().read(hash)
            }
            else -> null
        }
    }

    fun getBlockByNumber(params: List<Any?>): Mono<ByteArray>? {
        if (params.size != 2 || params[0] == null || params[1] == null) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 2 parameters")
        }
        val number: Long
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
                blockRef == "pending" -> {
                    return null
                }
                else -> {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Block number is invalid")
                }
            }
        } catch (e: IllegalArgumentException) {
            throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be a block number")
        }
        val withTx = params[1].toString().toBoolean()
        var block = reader.blocksByHeightAsCont()
            .read(number)
        block = if (withTx) {
            block.flatMap {
                fullBlocksReader.read(it.hash)
            }
        } else {
            block
        }
        return block.map { it.json!! }
    }
}
