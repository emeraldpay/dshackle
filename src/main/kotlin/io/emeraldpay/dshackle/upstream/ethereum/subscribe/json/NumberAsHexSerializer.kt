/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe.json

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import io.emeraldpay.etherjar.hex.HexQuantity
import java.math.BigInteger

/**
 * Encodes numeric values as hex string prefixed with <code>0x</code>, per Ethereum standard.
 */
class NumberAsHexSerializer : JsonSerializer<Number>() {

    override fun serialize(value: Number?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
            return
        }
        val hex = if (value is BigInteger) {
            HexQuantity.from(value)
        } else {
            HexQuantity.from(value.toLong())
        }
        gen.writeString(hex.toHex())
    }

}