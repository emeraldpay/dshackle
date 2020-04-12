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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson

open class BroadcastQuorum(
        jacksonRpcConverter: JacksonRpcConverter,
        val quorum: Int = 3
): CallQuorum, ValueAwareQuorum<String>(jacksonRpcConverter, String::class.java) {

    private var result: ByteArray? = null
    private var txid: String? = null
    private var calls = 0

    override fun init(head: Head<BlockJson<TransactionRefJson>>) {
    }

    override fun isResolved(): Boolean {
        return calls >= quorum
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun recordValue(response: ByteArray, responseValue: String?, upstream: Upstream<*, *>) {
        calls++
        if (txid == null && responseValue != null) {
            txid = responseValue
            result = response
        }
    }

    override fun recordError(response: ByteArray?, errorMessage: String?, upstream: Upstream<*, *>) {
        // can be "message: known transaction: TXID", "Transaction with the same hash was already imported" or "message: Nonce too low"
        calls++
        if (result == null) {
            result = response
        }
    }

}