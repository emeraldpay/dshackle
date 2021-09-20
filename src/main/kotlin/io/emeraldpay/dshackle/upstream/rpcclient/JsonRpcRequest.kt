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
package io.emeraldpay.dshackle.upstream.rpcclient

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import io.emeraldpay.dshackle.Global

data class JsonRpcRequest(
        val method: String,
        val params: List<Any?>,
        val id: Int
) {

    constructor(method: String, params: List<Any?>) : this(method, params, 1)

    fun toJson(): ByteArray {
        val json = mapOf(
                "jsonrpc" to "2.0",
                "id" to id,
                "method" to method,
                "params" to params
        )
        return Global.objectMapper.writeValueAsBytes(json)
    }

    override fun toString(): String {
        return String(this.toJson())
    }

    class Deserializer : JsonDeserializer<JsonRpcRequest>() {

        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): JsonRpcRequest {
            val node: JsonNode = p.readValueAsTree()
            val id = node.get("id").intValue()
            val method = node.get("method").textValue()
            val params = node.get("params").map {
                if (it.isNumber) {
                    it.asInt()
                } else if (it.isTextual) {
                    it.textValue()
                } else if (it.isBoolean) {
                    it.booleanValue()
                } else if (it.isNull) {
                    null
                } else {
                    throw IllegalStateException("Unsupported param type: ${it.asToken()}")
                }
            }
            return JsonRpcRequest(method, params, id)
        }

    }
}