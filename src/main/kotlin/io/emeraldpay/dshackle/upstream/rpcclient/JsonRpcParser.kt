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

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import io.infinitape.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory

class JsonRpcParser() {

    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcParser::class.java)
    }

    private val jsonFactory = JsonFactory()

    fun parse(json: ByteArray): JsonRpcResponse {
        val parser: JsonParser = jsonFactory.createParser(json)
        parser.nextToken()
        if (parser.currentToken != JsonToken.START_OBJECT) {
            println("token ${parser.currentToken}")
            return JsonRpcResponse(null, JsonRpcResponse.ResponseError(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Invalid JSON"))
        }
        var nullResponse: JsonRpcResponse? = null
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            val field = parser.currentName
            if (field == "jsonrpc" || field == "id") {
                if (!parser.nextToken().isScalarValue) {
                    return JsonRpcResponse(null, JsonRpcResponse.ResponseError(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Invalid JSON (id/type)"))
                }
                // just skip the field
            } else if (field == "result") {
                val value = parser.nextToken()
                val start = parser.tokenLocation
                if (value.isScalarValue) {
                    val text = parser.text
                    if (value == JsonToken.VALUE_STRING) {
                        return JsonRpcResponse(("\"" + text + "\"").toByteArray(), null)
                    } else if (value == JsonToken.VALUE_NULL) {
                        //if null we should check if error is present
                        nullResponse = JsonRpcResponse(text.toByteArray(), null)
                    } else {
                        return JsonRpcResponse(text.toByteArray(), null)
                    }
                } else if (value == JsonToken.START_OBJECT || value == JsonToken.START_ARRAY) {
                    parser.skipChildren()
                    val end = parser.currentLocation.byteOffset.toInt()
                    val copy = ByteArray((end - start.byteOffset).toInt())
                    System.arraycopy(json, start.byteOffset.toInt(), copy, 0, copy.size)
                    return JsonRpcResponse(copy, null)
                }
            } else if (field == "error") {
                val err = readError(parser)
                if (err != null) {
                    return JsonRpcResponse(null, err)
                }
            }
        }
        if (nullResponse != null) {
            return nullResponse
        }
        return JsonRpcResponse(null, JsonRpcResponse.ResponseError(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Invalid JSON structure"))
    }

    fun readError(parser: JsonParser): JsonRpcResponse.ResponseError? {
        var code = 0
        var message = ""

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.currentToken() == JsonToken.VALUE_NULL) {
                // error is just null
                return null
            }
            val field = parser.currentName()
            if (field == "code" && parser.currentToken == JsonToken.VALUE_NUMBER_INT) {
                code = parser.intValue
            } else if (field == "message" && parser.currentToken == JsonToken.VALUE_STRING) {
                message = parser.valueAsString
            }
        }

        return JsonRpcResponse.ResponseError(code, message)
    }
}