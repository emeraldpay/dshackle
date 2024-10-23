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
package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.rpcclient.CallParams
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams

data class ChainRequest(
    val method: String,
    val params: CallParams,
    val id: Int,
    val nonce: Long?,
    val selector: BlockchainOuterClass.Selector?,
    val isStreamed: Boolean = false,
    val matcher: Selector.Matcher = Selector.empty,
) {

    @JvmOverloads constructor(
        method: String,
        params: CallParams,
        nonce: Long? = null,
        selectors: BlockchainOuterClass.Selector? = null,
        isStreamed: Boolean = false,
        matcher: Selector.Matcher = Selector.empty,
    ) : this(method, params, 1, nonce, selectors, isStreamed, matcher)

    constructor(
        method: String,
        params: CallParams,
        matcher: Selector.Matcher,
    ) : this(method, params, 1, null, null, false, matcher)

    fun toJson(): ByteArray {
        return params.toJson(id, method)
    }

    override fun toString(): String {
        return String(this.toJson())
    }

    @Suppress("UNCHECKED_CAST")
    class Deserializer : JsonDeserializer<ChainRequest>() {

        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ChainRequest {
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
            return ChainRequest(method, ListParams(params as List<Any>), id, null, null)
        }
    }
}
