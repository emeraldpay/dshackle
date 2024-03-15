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

import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

data class ChainCallError(val code: Int, val message: String, val details: Any?) {

    constructor(code: Int, message: String) : this(code, message, null)

    companion object {
        @JvmStatic
        fun from(err: RpcException): ChainCallError {
            return ChainCallError(
                err.code,
                err.rpcMessage,
                err.details,
            )
        }
    }

    fun asException(id: ChainResponse.Id?): ChainException {
        return ChainCallUpstreamException(id ?: ChainResponse.NumberId(-1), this)
    }

    fun asException(id: ChainResponse.Id?, upstreamId: String?): ChainException {
        return ChainException(id ?: ChainResponse.NumberId(-1), this, upstreamId, false)
    }
}
