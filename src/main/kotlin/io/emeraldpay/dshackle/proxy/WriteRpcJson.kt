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
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.ResponseJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.lang.StringBuilder
import java.time.Duration
import java.util.function.Function

/**
 * Writer for JSON RPC requests
 */
@Service
open class WriteRpcJson(
        @Autowired private val objectMapper: ObjectMapper
) {

    companion object {
        private val log = LoggerFactory.getLogger(WriteRpcJson::class.java)
    }

    /**
     * Convert Dshackle protobuf based responses to JSON RPC formatted as strings
     */
    open fun toJsons(call: ProxyCall): Function<Flux<BlockchainOuterClass.NativeCallReplyItem>, Flux<String>> {
        return Function { flux ->
            flux.flatMap { response ->
                val json = ResponseJson<Any, Any>()
                if (!call.ids.containsKey(response.id)) {
                    log.warn("ID wasn't requested: ${response.id}")
                    return@flatMap Flux.empty<String>()
                }
                json.id = call.ids[response.id]
                if (response.succeed) {
                    val payload = objectMapper.readValue(response.payload.toByteArray(), ResponseJson::class.java)
                    if (payload.error != null) {
                        json.error = payload.error
                    } else {
                        json.result = payload.result
                    }
                } else {
                    json.error = RpcResponseError(-32002, response.errorMessage)
                }
                Flux.just(objectMapper.writeValueAsString(json))
            }.onErrorContinue { t, u ->
                log.warn("Failed to convert to JSON", t)
            }
        }
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