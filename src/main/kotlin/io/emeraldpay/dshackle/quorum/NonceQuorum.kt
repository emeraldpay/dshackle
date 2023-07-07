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
import io.emeraldpay.etherjar.hex.HexQuantity
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min

open class NonceQuorum(
    maxTries: Int = 3
) : CallQuorum, ValueAwareQuorum<String>(String::class.java) {

    private val lock = ReentrantLock()
    private var resultValue = 0L
    private var result: ByteArray? = null
    private var receivedTimes = 0
    private var errors = 0
    private var sig: ResponseSigner.Signature? = null
    private var tries = maxTries

    override fun init(head: Head) {
    }

    override fun setTotalUpstreams(total: Int) {
        tries = min(total, tries).coerceAtLeast(1)
    }

    override fun isResolved(): Boolean {
        lock.withLock {
            return receivedTimes >= tries && !isFailed()
        }
    }

    override fun isFailed(): Boolean {
        return errors >= tries
    }
    override fun getSignature(): ResponseSigner.Signature? {
        return sig
    }

    override fun recordValue(response: ByteArray, responseValue: String?, signature: ResponseSigner.Signature?, upstream: Upstream) {
        val value = responseValue?.let { str ->
            HexQuantity.from(str).value.toLong()
        }
        lock.withLock {
            receivedTimes++
            if (value != null && value > resultValue) {
                resultValue = value
                result = response
                sig = signature
            } else if (result == null) {
                result = response
                sig = signature
            }
        }
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun recordError(response: ByteArray?, errorMessage: String?, signature: ResponseSigner.Signature?, upstream: Upstream) {
        errors++
    }

    override fun toString(): String {
        return "Quorum: Confirm with $tries upstreams"
    }
}
