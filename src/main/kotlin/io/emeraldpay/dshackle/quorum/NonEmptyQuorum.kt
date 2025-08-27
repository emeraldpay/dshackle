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

open class NonEmptyQuorum(
    val maxTries: Int = 3,
) : ValueAwareQuorum<Any>(Any::class.java),
    CallQuorum {
    private var result: ByteArray? = null
    private var tries: Int = 0
    private var sig: ResponseSigner.Signature? = null

    override fun init(head: Head) {
    }

    override fun setTotalUpstreams(total: Int) {
        // ignore the number because NonEmpty is supposed to retry if some data is empty because it just not yet available
    }

    override fun isResolved(): Boolean = result != null

    override fun isFailed(): Boolean = tries >= maxTries

    override fun getSignature(): ResponseSigner.Signature? = sig

    override fun recordValue(
        response: ByteArray,
        responseValue: Any?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        tries++
        if (responseValue != null) {
            result = response
            sig = signature
        }
    }

    override fun getResult(): ByteArray? = result

    override fun recordError(
        response: ByteArray?,
        errorMessage: String?,
        sig: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        tries++
    }

    override fun toString(): String = "Quorum: Accept Non Error Result"
}
