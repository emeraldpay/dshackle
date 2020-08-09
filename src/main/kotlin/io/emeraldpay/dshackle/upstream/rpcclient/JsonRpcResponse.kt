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

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import io.infinitape.etherjar.rpc.RpcException
import reactor.core.publisher.Mono

class JsonRpcResponse(
        private val result: ByteArray?,
        val error: ResponseError?,
        val id: Id
) {

    constructor(result: ByteArray?, error: ResponseError?) : this(result, error, IntId(0))

    companion object {
        private val NULL_VALUE = "null".toByteArray()

        @JvmStatic
        fun ok(value: ByteArray): JsonRpcResponse {
            return JsonRpcResponse(value, null)
        }

        @JvmStatic
        fun ok(value: ByteArray, id: Id): JsonRpcResponse {
            return JsonRpcResponse(value, null, id);
        }

        @JvmStatic
        fun ok(value: String): JsonRpcResponse {
            return JsonRpcResponse(value.toByteArray(), null)
        }

        @JvmStatic
        fun error(code: Int, msg: String): JsonRpcResponse {
            return JsonRpcResponse(null, ResponseError(code, msg))
        }

        @JvmStatic
        fun error(code: Int, msg: String, id: Id): JsonRpcResponse {
            return JsonRpcResponse(null, ResponseError(code, msg), id)
        }
    }

    fun hasResult(): Boolean {
        return result != null
    }

    fun hasError(): Boolean {
        return error != null
    }

    fun isNull(): Boolean {
        return result != null && NULL_VALUE.contentEquals(result)
    }

    fun getResult(): ByteArray {
        return result ?: ByteArray(0)
    }

    fun getResultAsRawString(): String {
        return String(getResult())
    }

    fun getResultAsProcessedString(): String {
        val str = getResultAsRawString()
        if (str.startsWith("\"") && str.endsWith("\"")) {
            return str.substring(1, str.length - 1)
        }
        throw IllegalStateException("Not as JS string")
    }

    fun requireResult(): Mono<ByteArray> {
        return if (error != null) {
            Mono.error(error.asException())
        } else {
            Mono.just(getResult())
        }
    }

    fun requireStringResult(): Mono<String> {
        return if (error != null) {
            Mono.error(error.asException())
        } else {
            Mono.just(getResultAsProcessedString())
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JsonRpcResponse) return false

        if (result != null) {
            if (other.result == null) return false
            if (!result.contentEquals(other.result)) return false
        } else if (other.result != null) return false
        if (error != other.error) return false

        return true
    }

    override fun hashCode(): Int {
        var result1 = result?.contentHashCode() ?: 0
        result1 = 31 * result1 + (error?.hashCode() ?: 0)
        return result1
    }

    class ResponseError(val code: Int, val message: String) {
        fun asException(): RpcException {
            return RpcException(code, message)
        }
    }

    /**
     * JSON RPC wrapper. Makes sure that the id is either Int or String
     */
    interface Id {
        fun asInt(): Int
        fun asString(): String
        fun isInt(): Boolean

        companion object {
            @JvmStatic
            fun from(id: Any): Id {
                if (id is Int) {
                    return IntId(id)
                }
                if (id is Number) {
                    return IntId(id.toInt())
                }
                if (id is String) {
                    return StringId(id)
                }
                throw IllegalArgumentException("Id must be Int or String")
            }
        }
    }

    class IntId(val id: Int) : Id {
        override fun asInt(): Int {
            return id
        }

        override fun asString(): String {
            throw IllegalStateException("Not string")
        }

        override fun isInt(): Boolean {
            return true
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is IntId) return false

            if (id != other.id) return false

            return true
        }

        override fun hashCode(): Int {
            return id
        }
    }

    class StringId(val id: String) : Id {
        override fun asInt(): Int {
            throw IllegalStateException("Not int")
        }

        override fun asString(): String {
            return id
        }

        override fun isInt(): Boolean {
            return false
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is StringId) return false

            if (id != other.id) return false

            return true
        }

        override fun hashCode(): Int {
            return id.hashCode()
        }

    }

    class ResponseJsonSerializer : JsonSerializer<JsonRpcResponse>() {
        override fun serialize(value: JsonRpcResponse, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeStartObject()
            gen.writeStringField("jsonrpc", "2.0")
            if (value.id.isInt()) {
                gen.writeNumberField("id", value.id.asInt())
            } else {
                gen.writeStringField("id", value.id.asString())
            }
            if (value.error != null) {
                gen.writeObjectFieldStart("error")
                gen.writeNumberField("code", value.error.code)
                gen.writeStringField("message", value.error.message)
                gen.writeEndObject()
            } else {
                if (value.result == null) {
                    throw IllegalStateException("No result set")
                }
                gen.writeFieldName("result")
                gen.writeRaw(":")
                gen.writeRaw(String(value.result))
            }
            gen.writeEndObject()
        }
    }
}