/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.proxy

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.function.Function

/**
 * Writer for JSON RPC requests
 */
@Service
open class WriteRpcJson {

    companion object {
        private val log = LoggerFactory.getLogger(WriteRpcJson::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    /**
     * Convert Dshackle protobuf based responses to JSON RPC formatted as strings
     */
    open fun toJsons(call: ProxyCall): Function<Flux<NativeCall.CallResult>, Flux<String>> {
        return Function { flux ->
            flux
                .flatMap { response ->
                    if (call.ids.size <= response.id) {
                        log.warn("ID wasn't requested: ${response.id}")
                        return@flatMap Flux.empty<String>()
                    }
                    val json = toJson(call, response)
                    if (json == null) {
                        Flux.empty<String>()
                    } else {
                        Flux.just(json)
                    }
                }
                .onErrorResume { t ->
                    if (t is NativeCall.CallFailure) {
                        Mono.just(toJson(call, t)!!)
                    } else {
                        Mono.empty()
                    }
                }
        }
    }

    open fun toJson(call: ProxyCall, response: NativeCall.CallResult): String? {
        val id = call.ids[response.id]?.let {
            JsonRpcResponse.Id.from(it)
        } ?: return null
        val json = if (response.isError()) {
            val error = response.error!!
            error.upstreamError?.let { upstreamError ->
                JsonRpcResponse.error(upstreamError, id)
            } ?: JsonRpcResponse.error(-32002, error.message, id)
        } else {
            JsonRpcResponse.ok(response.result!!, id)
        }
        return objectMapper.writeValueAsString(json)
    }

    fun toJson(call: ProxyCall, error: NativeCall.CallFailure): String? {
        val id = call.ids[error.id] ?: return null
        val json = JsonRpcResponse.error(-32003, error.reason.message ?: "", JsonRpcResponse.Id.from(id))
        return objectMapper.writeValueAsString(json)
    }

    /**
     * Format response as JSON Array, for Batch requests
     */
    fun asArray(): Function<Flux<String>, Flux<String>> {
        return Function { flux ->
            val body = flux.zipWith(Flux.concat(Mono.just(false), Flux.just(true).repeat()))
                .map {
                    if (it.t2) {
                        "," + it.t1
                    } else {
                        it.t1
                    }
                }
            Flux.concat(
                Mono.just("["),
                body,
                Mono.just("]")
            )
        }
    }
}
