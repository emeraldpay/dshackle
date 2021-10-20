/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle.proxy

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.etherjar.rpc.json.RequestJson
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.IOException
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Reader for JSON RPC request
 */
@Service
open class ReadRpcJson : Function<ByteArray, ProxyCall> {

    companion object {
        private val log = LoggerFactory.getLogger(ReadRpcJson::class.java)
        private val spaces = " \n\t".toByteArray()
    }

    private val jsonExtractor: Function<Map<*, *>, RequestJson<Any>>
    private val objectMapper: ObjectMapper = Global.objectMapper

    init {
        jsonExtractor = Function { json ->
            if (json["id"] == null) {
                throw RpcException(RpcResponseError.CODE_INVALID_REQUEST, "ID is not set")
            }
            val id = json["id"]
            if ("2.0" != json["jsonrpc"]) {
                if (json["jsonrpc"] == null) {
                    throw RpcException(
                        RpcResponseError.CODE_INVALID_REQUEST,
                        "jsonrpc version is not set",
                        id?.let { JsonRpcResponse.Id.from(it) }
                    )
                }
                throw RpcException(
                    RpcResponseError.CODE_INVALID_REQUEST,
                    "Unsupported JSON RPC version: " + json["jsonrpc"].toString(),
                    id?.let { JsonRpcResponse.Id.from(it) }
                )
            }
            if (!(json["method"] != null && json["method"] is String)) {
                throw RpcException(
                    RpcResponseError.CODE_INVALID_REQUEST,
                    "Method is not set",
                    id?.let { JsonRpcResponse.Id.from(it) }
                )
            }
            if (json.containsKey("params") && json["params"] !is List<*>) {
                throw RpcException(
                    RpcResponseError.CODE_INVALID_REQUEST,
                    "Params must be an array",
                    id?.let { JsonRpcResponse.Id.from(it) }
                )
            }
            RequestJson<Any>(
                json["method"].toString(),
                // params MAY be omitted
                (json["params"] ?: emptyList<Any>()) as List<*>,
                id
            )
        }
    }

    /**
     * Read fist non-space character, which supposed to start actual JSON part of the request
     */
    @Throws(IOException::class)
    fun getStartOfJson(buf: ByteArray): Byte {
        val count = buf.size
        var i = 0
        // if cannot find anything in the first 255 bytes, just consider it as invalid
        while (i < 256 && i < count) {
            if (buf[i] != spaces[0] && buf[i] != spaces[1] && buf[i] != spaces[2]) {
                return buf[i]
            }
            i++
        }
        throw IllegalArgumentException("Invalid input")
    }

    /**
     * Check the type of the payload, based on the format (first character at this case)
     */
    @Throws(IOException::class)
    fun getType(data: ByteArray): ProxyCall.RpcType {
        val first = try {
            getStartOfJson(data)
        } catch (e: IllegalArgumentException) {
            throw RpcException(RpcResponseError.CODE_INVALID_JSON, "Empty JSON")
        }
        if (first == '{'.code.toByte()) {
            return ProxyCall.RpcType.SINGLE
        } else if (first == '['.code.toByte()) {
            return ProxyCall.RpcType.BATCH
        }
        throw RpcException(RpcResponseError.CODE_INVALID_JSON, "Failed to parse JSON")
    }

    /**
     * Convert payload to the proxy call details
     */
    override fun apply(data: ByteArray): ProxyCall {
        val list: MutableList<Map<*, *>>
        try {
            val type = getType(data)
            if (ProxyCall.RpcType.BATCH == type) {
                list = objectMapper.readerFor(MutableList::class.java).readValue(data)
            } else {
                list = ArrayList(1)
                val json = objectMapper.readerFor(MutableMap::class.java).readValue<Map<*, *>>(data)
                list.add(json)
            }
            val context = ProxyCall(type)
            // our internal ids for calls
            var seq = 0
            val batch = list.stream()
                .map<RequestJson<Any>>(jsonExtractor)
                .map { json ->
                    val id = seq++
                    context.ids[id] = json.id
                    BlockchainOuterClass.NativeCallItem.newBuilder()
                        .setId(id)
                        .setMethod(json.method)
                        .setPayload(ByteString.copyFrom(objectMapper.writeValueAsBytes(json.params)))
                        .build()
                }
                .collect(Collectors.toList())
            context.items.addAll(batch)
            return context
        } catch (e: RpcException) {
            throw e
        } catch (e: Exception) {
            log.error("Parse Error: " + e.message)
            throw RpcException(RpcResponseError.CODE_INVALID_JSON, e.message)
        }
    }
}
