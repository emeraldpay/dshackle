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
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson

open class AlwaysQuorum: CallQuorum {

    private var resolved = false
    private var result: ByteArray? = null

    override fun init(head: Head<BlockJson<TransactionRefJson>>) {
    }

    override fun isResolved(): Boolean {
        return resolved
    }

    override fun record(response: ByteArray, upstream: Upstream<*, *>): Boolean {
        result = response
        resolved = true
        return true
    }

    override fun record(error: RpcException, upstream: Upstream<*, *>) {
    }

    override fun getResult(): ByteArray? {
        return result
    }
}