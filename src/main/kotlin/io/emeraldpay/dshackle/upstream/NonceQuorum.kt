/**
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
package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.json.BlockJson
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

open class NonceQuorum(
        jacksonRpcConverter: JacksonRpcConverter,
        val tries: Int = 3
): CallQuorum, ValueAwareQuorum<String>(jacksonRpcConverter, String::class.java) {

    private val lock = ReentrantLock()
    private var resultValue = 0L
    private var result: ByteArray? = null
    private var receivedTimes = 0
    private var errors = 0

    override fun init(head: Head<BlockJson<TransactionId>>) {
    }

    override fun isResolved(): Boolean {
        lock.withLock {
            return receivedTimes >= tries || errors >= tries
        }
    }

    override fun recordValue(response: ByteArray, responseValue: String?, upstream: Upstream) {
        val value = responseValue?.let { str ->
            HexQuantity.from(str).value.toLong()
        }
        lock.withLock {
            receivedTimes++
            if (value != null && value > resultValue) {
                resultValue = value
                result = response
            } else if (result == null) {
                result = response
            }
        }
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun recordError(response: ByteArray,  errorMessage: String?, upstream: Upstream) {
        errors++
    }

}