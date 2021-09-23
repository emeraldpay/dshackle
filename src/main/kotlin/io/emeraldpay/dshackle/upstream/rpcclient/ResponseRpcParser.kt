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

import org.slf4j.LoggerFactory

open class ResponseRpcParser() : ResponseParser<JsonRpcResponse>() {

    companion object {
        private val log = LoggerFactory.getLogger(ResponseRpcParser::class.java)
    }

    override fun build(state: Preparsed): JsonRpcResponse {
        if (state.error != null) {
            return JsonRpcResponse(null, state.error, state.id ?: JsonRpcResponse.Id.from(-1))
        }
        if (state.nullResult) {
            return JsonRpcResponse("null".toByteArray(), null, state.id ?: JsonRpcResponse.Id.from(-1))
        }
        return JsonRpcResponse(state.result, null, state.id ?: JsonRpcResponse.Id.from(-1))
    }

}