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

import io.emeraldpay.etherjar.rpc.RpcException

class JsonRpcError(val code: Int, val message: String, val details: Any?) {

    constructor(code: Int, message: String) : this(code, message, null)

    companion object {
        @JvmStatic
        fun from(err: RpcException): JsonRpcError {
            return JsonRpcError(
                err.code, err.rpcMessage, err.details
            )
        }
    }

    fun asException(id: JsonRpcResponse.Id?, method: String? = null): JsonRpcException {
        return JsonRpcException(id ?: JsonRpcResponse.NumberId(-1), this, method)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JsonRpcError) return false

        if (code != other.code) return false
        if (message != other.message) return false
        if (details != other.details) return false

        return true
    }

    override fun hashCode(): Int {
        var result = code
        result = 31 * result + message.hashCode()
        result = 31 * result + (details?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "JsonRpcError(code=$code, message='$message', details=$details)"
    }
}
