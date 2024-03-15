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

open class ChainException(
    val id: ChainResponse.Id,
    val error: ChainCallError,
    val upstreamId: String? = null,
    writableStackTrace: Boolean = true,
    cause: Throwable? = null,
) : Exception(error.message, cause, true, writableStackTrace) {

    constructor(id: Int, message: String) : this(ChainResponse.NumberId(id), ChainCallError(-32005, message))

    constructor(id: Int, message: String, cause: Throwable) : this(ChainResponse.NumberId(id), ChainCallError(-32005, message), cause = cause)

    companion object {
        fun from(err: RpcException): ChainException {
            val id = err.details?.let {
                if (it is ChainResponse.Id) {
                    it
                } else {
                    ChainResponse.NumberId(-3)
                }
            } ?: ChainResponse.NumberId(-4)
            return ChainException(
                id,
                ChainCallError.from(err),
            )
        }
    }
}
