/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner

interface CallQuorum {
    companion object {
        fun isConnectionUnavailable(statusCode: Int): Boolean {
            //
            // The problem is that some servers respond with 4xx/5xx in normal cases telling that the input data
            // cannot be processed (ex. transaction is known already), in addition to the error message in the JSON RPC body.
            // Such kind of error must be provided to the user, it's _the response_.
            //
            // Others, like Infura, may answer with 429. And it's not the final answer. So we don't record this error.
            //
            // Here is a workaround to catch commons HTTP errors, like `429 Too Many Requests` which mean a connection error
            // that may be ignored if there is another upstream that provides a valid response.
            //
            // 401 - Unauthorized
            // 429 - Too Many Requests
            // 502 - Bad Gateway
            // 503 - Service Unavailable
            // 504 - Gateway Timeout
            //
            // See https://github.com/emeraldpay/dshackle/issues/251 also
            //
            return statusCode == 429 || statusCode == 401 || statusCode in 502..504
        }

        fun isConnectionUnavailable(error: JsonRpcException): Boolean =
            error.statusCode != null && isConnectionUnavailable(error.statusCode)
    }

    /**
     * Called when the reader is done with processing, which may happen because of exhaustion of available upstreams.
     * That is the last moment when Quorum may decide if it's resolved or failed.
     */
    fun close() {}

    fun init(head: Head)

    fun setTotalUpstreams(total: Int)

    fun isResolved(): Boolean

    fun isFailed(): Boolean

    fun record(
        response: ByteArray,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean

    fun record(
        error: JsonRpcException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    )

    fun getSignature(): ResponseSigner.Signature?

    fun getResult(): ByteArray?

    fun getError(): JsonRpcError?
}
