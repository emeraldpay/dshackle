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
import io.emeraldpay.dshackle.upstream.CallMethods
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.infinitape.etherjar.rpc.*
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Duration

open class DirectEthereumApi(
        val rpcClient: ReactorRpcClient,
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
        return result
                .doOnError { t ->
                    log.warn("Upstream error: [${t.message}] for $method")
                }
                .map {
                    val resp = ResponseJson<Any, Int>()
                    resp.id = id
                    resp.result = it
                    objectMapper.writer().writeValueAsBytes(resp)
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

    private fun callUpstream(method: String, params: List<Any>): Mono<out Any> {
        return rpcClient.execute(RpcCall.create(method, Any::class.java, params))
                .timeout(timeout, Mono.error(RpcException(-32603, "Upstream timeout")))
    }
}