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
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import kotlin.math.min

open class BroadcastQuorum(
    maxQuorum: Int = 3,
) : ValueAwareQuorum<String>(String::class.java),
    CallQuorum {
    private var result: ByteArray? = null
    private var txid: String? = null
    private var calls = 0
    private var sig: ResponseSigner.Signature? = null
    var quorum: Int = maxQuorum

    private val isQuorumReached: Boolean
        get() = calls >= quorum

    override fun init(head: Head) {
    }

    override fun setTotalUpstreams(total: Int) {
        quorum = min(total, quorum).coerceAtLeast(1)
    }

    override fun isResolved(): Boolean = isQuorumReached && txid != null

    override fun isFailed(): Boolean = isQuorumReached && !isResolved()

    override fun getResult(): ByteArray? = result

    override fun getSignature(): ResponseSigner.Signature? = sig

    override fun recordValue(
        response: ByteArray,
        responseValue: String?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        calls++
        if (txid == null && responseValue != null) {
            txid = responseValue
            sig = signature
            result = response
        }
    }

    override fun recordError(
        response: ByteArray?,
        errorMessage: String?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        // can be "message: known transaction: TXID", "Transaction with the same hash was already imported" or "message: Nonce too low"
        calls++
        if (result == null) {
            result = response
            sig = signature
        }
    }

    override fun toString(): String = "Quorum: Broadcast to $quorum upstreams"
}
