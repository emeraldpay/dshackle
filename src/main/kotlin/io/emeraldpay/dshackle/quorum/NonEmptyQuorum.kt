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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.RpcException

open class NonEmptyQuorum(
        val maxTries: Int = 3
) : CallQuorum, ValueAwareQuorum<Any>(Any::class.java) {

    private var result: ByteArray? = null
    private var tries: Int = 0

    override fun init(head: Head) {
    }

    override fun isResolved(): Boolean {
        return result != null
    }

    override fun isFailed(): Boolean {
        return tries >= maxTries
    }

    override fun recordValue(response: ByteArray, responseValue: Any?, upstream: Upstream) {
        tries++
        if (responseValue != null) {
            result = response
        }
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun recordError(response: ByteArray?, errorMessage: String?, upstream: Upstream) {
        tries++
    }

    override fun record(error: RpcException, upstream: Upstream) {
        tries++
    }

}