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

import io.emeraldpay.dshackle.Global

class JsonRpcRequest(
        val method: String,
        val params: List<Any>
) {

    fun toJson(): ByteArray {
        val json = mapOf(
                "jsonrpc" to "2.0",
                "id" to 1,
                "method" to method,
                "params" to params
        )
        return Global.objectMapper.writeValueAsBytes(json)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JsonRpcRequest) return false

        if (method != other.method) return false
        if (params != other.params) return false

        return true
    }

    override fun hashCode(): Int {
        var result = method.hashCode()
        result = 31 * result + params.hashCode()
        return result
    }


}