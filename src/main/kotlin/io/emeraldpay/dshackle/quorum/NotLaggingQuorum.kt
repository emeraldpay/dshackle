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
import java.util.concurrent.atomic.AtomicReference

/**
 * Accepts a response only from a "synced" upstreams, where "maxLag" specifies how many blocks it may be behind to be considered as "synced"
 *
 * NOTE: NativeCall checks the quorums and applies a HeightSelector if NotLaggingQuorum is enabled for a call
 */
class NotLaggingQuorum(
    val maxLag: Long = 0,
) : CallQuorum {
    private val result: AtomicReference<ByteArray> = AtomicReference()
    private val failed = AtomicReference(false)
    private var rpcError: JsonRpcError? = null
    private var sig: ResponseSigner.Signature? = null

    override fun init(head: Head) {
    }

    override fun setTotalUpstreams(total: Int) {
    }

    override fun isResolved(): Boolean = !isFailed() && result.get() != null

    override fun isFailed(): Boolean = failed.get()

    override fun record(
        response: ByteArray,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean {
        val lagging = upstream.getLag() > maxLag
        if (!lagging) {
            result.set(response)
            sig = signature
            return true
        }
        return false
    }

    override fun record(
        error: JsonRpcException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        this.rpcError = error.error
        val lagging = upstream.getLag() > maxLag
        if (!lagging && result.get() == null) {
            failed.set(true)
        }
    }

    override fun getSignature(): ResponseSigner.Signature? = sig

    override fun getResult(): ByteArray = result.get()

    override fun getError(): JsonRpcError? = rpcError

    override fun toString(): String = "Quorum: late <= $maxLag blocks"
}
