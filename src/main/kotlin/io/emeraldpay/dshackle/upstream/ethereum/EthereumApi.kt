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
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.rpc.*
import reactor.core.publisher.Mono
import java.io.InputStream

abstract class EthereumApi(
        objectMapper: ObjectMapper
) {

    private val jacksonRpcConverter = JacksonRpcConverter(objectMapper)
    var upstream: Upstream? = null

    abstract fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray>

    fun <JS, RS> executeAndConvert(rpcCall: RpcCall<JS, RS>): Mono<RS> {
        val convertToJS = java.util.function.Function<ByteArray, Mono<JS>> { resp ->
            val inputStream: InputStream = resp.inputStream()
            val jsonValue: JS? = jacksonRpcConverter.fromJson(inputStream, rpcCall.jsonType, Int::class.java)
            if (jsonValue == null) Mono.empty<JS>()
            else Mono.just(jsonValue)
        }
        return execute(0, rpcCall.method, rpcCall.params as List<Any>)
                .flatMap(convertToJS)
                .map(rpcCall.converter::apply)
    }
}