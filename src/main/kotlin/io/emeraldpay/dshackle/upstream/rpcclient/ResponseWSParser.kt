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
package io.emeraldpay.dshackle.upstream.rpcclient

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import org.slf4j.LoggerFactory
import java.io.IOException

class ResponseWSParser : ResponseParser<ResponseWSParser.WsResponse>() {

    companion object {
        private val log = LoggerFactory.getLogger(ResponseWSParser::class.java)
        private val NULL_RESULT = "null".toByteArray()
    }

    override fun build(state: Preparsed): WsResponse {
        if (state.isRpcReady) {
            return WsResponse(
                Type.RPC,
                state.id!!,
                if (state.nullResult) NULL_RESULT else state.result,
                state.error,
            )
        }
        if (state.isSubReady) {
            return WsResponse(
                Type.SUBSCRIPTION,
                JsonRpcResponse.Id.from(state.subId!!),
                if (state.nullResult) NULL_RESULT else state.result,
                state.error,
            )
        }
        throw IllegalStateException("State is not ready")
    }

    override fun process(parser: JsonParser, json: ByteArray, field: String, state: Preparsed): Preparsed {
        if ("method" == field) {
            parser.nextToken()
            val method = parser.valueAsString
            return state.copy(subMethod = method)
        }
        if ("params" == field) {
            // example:
            // newHeads
            // {
            //  "jsonrpc": "2.0",
            //  "method": "eth_subscription",
            //  "params": {
            //    "result": {
            //      "difficulty": ......
            //    },
            //    "subscription": "...."
            //  }
            // }
            return decodeSubscription(parser, json, state)
        }
        return super.process(parser, json, field, state)
    }

    @Throws(IOException::class)
    private fun decodeString(parser: JsonParser): String {
        if (parser.currentToken() != JsonToken.VALUE_STRING) {
            parser.nextToken()
        }
        check(parser.currentToken().isScalarValue) { "Id is not a string" }
        return parser.valueAsString
    }

    @Throws(IOException::class)
    protected fun decodeSubscription(parser: JsonParser, json: ByteArray, stateOriginal: Preparsed): Preparsed {
        var state = stateOriginal
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            checkNotNull(parser.currentToken()) { "JSON finished before data received" }
            val field = parser.currentName()
            if ("subscription" == field) {
                state = state.copy(subId = decodeString(parser))
            } else if ("result" == field) {
                state = state.copy(result = readResult(json, parser))
            }
        }
        return state
    }

    enum class Type {
        SUBSCRIPTION, RPC
    }

    data class WsResponse(
        val type: Type,
        val id: JsonRpcResponse.Id,
        val value: ByteArray?,
        val error: JsonRpcError?,
    )
}
