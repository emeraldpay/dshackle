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
package io.emeraldpay.dshackle.upstream.bitcoin.data

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import java.math.BigDecimal

class RpcUnspentDeserializer : JsonDeserializer<RpcUnspent>() {

    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): RpcUnspent {
        val node: JsonNode = jp.readValueAsTree()
        return RpcUnspent(
            node.get("txid").asText(),
            node.get("vout").asInt(),
            node.get("address").asText(),
            BigDecimal(node.get("amount").asText()).multiply(BigDecimal.TEN.pow(8)).longValueExact(),
        )
    }
}
