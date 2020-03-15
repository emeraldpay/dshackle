/**
 * Copyright (c) 2019 ETCDEV GmbH
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
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.math.BigInteger

open class DirectEthereumApi(
        val rpcClient: ReactorRpcClient,
        var caches: Caches?,
        private val objectMapper: ObjectMapper,
        val targets: CallMethods
): EthereumApi(objectMapper) {

    var timeout = Defaults.timeout
    private val log = LoggerFactory.getLogger(EthereumApi::class.java)

    override fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        val result: Mono<out Any> = when {
            targets.isHardcoded(method) -> Mono.just(method).map { targets.executeHardcoded(it) }
            targets.isAllowed(method)   -> callUpstream(method, params)
            else -> Mono.error(RpcException(-32601, "Method not allowed or not found"))
        }
        return processResult(id, method, result)
    }

    public fun processResult(id: Int, method: String, result: Mono<out Any>): Mono<ByteArray> {
        return result
                .doOnError { t ->
                    log.warn("Upstream error: [${t.message}] for $method")
                }
                .map {
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.result = it
                    resp
                }
                .switchIfEmpty(
                        Mono.fromCallable {
                            val resp = ResponseJson<Any, Int>()
                            resp.id = id
                            resp.result = null
                            resp
                        }
                )
                .map {
                    objectMapper.writer().writeValueAsBytes(it)
                }
                .onErrorResume(StatusRuntimeException::class.java) { t ->
                    if (t.status.code == Status.Code.CANCELLED) {
                        Mono.empty<ByteArray>()
                    } else {
                        Mono.error(RpcException(RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR, "gRPC error ${t.status}"))
                    }
                }
                .onErrorMap { t ->
                    if (RpcException::class.java.isAssignableFrom(t.javaClass)) {
                        t
                    } else {
                        log.warn("Convert to RPC error. Exception ${t.javaClass}:${t.message}", t)
                        RpcException(-32020, "Error reading from upstream", null, t)
                    }
                }
                .onErrorResume(RpcException::class.java) { t ->
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.error = t.error
                    Mono.just(objectMapper.writer().writeValueAsBytes(resp))
                }
    }

    /**
     * Actual request to the remote endpoint
     */
    private fun callUpstream(method: String, params: List<Any>): Mono<out Any> {
        return rpcClient.execute(callMapping(method, params))
                .timeout(timeout, Mono.error(RpcException(-32603, "Upstream timeout")))
                .doOnNext { value ->
                    try {
                        caches?.cacheRequested(value)
                    } catch (e: Throwable) {
                        //ignore all caching errors, client shouldn't have problems because of them
                        log.warn("Uncaught caching exception", e)
                    }
                }
    }

    /**
     * Prepare RpcCall with data types specific for that particular requests. In general it may return a call that just
     * parses JSON into Map. But the purpose of further processing and caching for some of the requests we want
     * to have actual data types.
     */
    fun callMapping(method: String, params: List<Any>): RpcCall<out Any, out Any> {
        return when {
            method == "eth_getTransactionByHash" -> {
                if (params.size != 1) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 1 parameter")
                }
                val hash: TransactionId
                try {
                    hash = TransactionId.from(params[0].toString())
                } catch (e: IllegalArgumentException) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be transaction id")
                }
                Commands.eth().getTransaction(hash)
            }
            method == "eth_getBlockByHash" -> {
                if (params.size != 2) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "Must provide 2 parameters")
                }
                val hash: BlockHash
                try {
                    hash = BlockHash.from(params[0].toString())
                } catch (e: IllegalArgumentException) {
                    throw RpcException(RpcResponseError.CODE_INVALID_METHOD_PARAMS, "[0] must be block hash")
                }
                val withTx = params[1].toString().toBoolean()
                if (withTx) {
                    Commands.eth().getBlockWithTransactions(hash)
                } else {
                    Commands.eth().getBlock(hash)
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
                    Commands.eth().getBlockWithTransactions(number)
                } else {
                    Commands.eth().getBlock(number)
                }
            }
            else -> RpcCall.create(method, Any::class.java, params)
        }
    }
}