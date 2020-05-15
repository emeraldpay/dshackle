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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.math.BigInteger

class NativeCallRouter(
        private val objectMapper: ObjectMapper,
        private val reader: EthereumReader,
        private val methods: CallMethods
) : Reader<JsonRpcRequest, JsonRpcResponse> {

    companion object {
        private val log = LoggerFactory.getLogger(NativeCallRouter::class.java)
    }

    private val fullBlocksReader = EthereumFullBlocksReader(
            objectMapper,
            reader.blocksByIdAsCont(),
            reader.txByHashAsCont()
    )

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        if (methods.isHardcoded(key.method)) {
            return Mono.just(methods.executeHardcoded(key.method))
                    .map { JsonRpcResponse(it, null) }
        }
        if (!methods.isAllowed(key.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
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
                if (params.size != 2) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 2 parameters")
                }
                val number: Long
                try {
                    val quantity = HexQuantity.from(params[0].toString()) ?: throw IllegalArgumentException()
                    number = quantity.value.let {
                        if (it < BigInteger.valueOf(Long.MAX_VALUE) && it >= BigInteger.ZERO) {
                            it.toLong()
                        } else {
                            throw IllegalArgumentException()
                        }
                    }
                } catch (e: IllegalArgumentException) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be block number")
                }
                val withTx = params[1].toString().toBoolean()
                if (withTx) {
                    log.warn("Block by number is not implemented")
                    null
                } else {
                    reader.blocksByHeightAsCont().read(number).map { it.json!! }
                }
            }
            else -> null
        }
    }
}