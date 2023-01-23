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
import java.util.concurrent.ConcurrentLinkedQueue

open class AlwaysQuorum : CallQuorum {

    private var resolved = false
    private var result: ByteArray? = null
    private var rpcError: JsonRpcError? = null
    private var sig: ResponseSigner.Signature? = null
    private var providedUpstreamId: String? = null
    private val resolvers: MutableCollection<Upstream> = ConcurrentLinkedQueue()

    override fun init(head: Head) {
    }

    override fun isResolved(): Boolean {
        return resolved
    }

    override fun isFailed(): Boolean {
        return rpcError != null
    }

    override fun getSignature(): ResponseSigner.Signature? {
        return sig
    }

    override fun getProvidedUpstreamId(): String? {
        return providedUpstreamId
    }

    override fun record(
        response: ByteArray,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
        providedUpstreamId: String?
    ): Boolean {
        result = response
        resolved = true
        sig = signature
        resolvers.add(upstream)
        this.providedUpstreamId = providedUpstreamId
        return true
    }

    override fun record(
        error: JsonRpcException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream
    ) {
        this.rpcError = error.error
        sig = signature
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun getError(): JsonRpcError? {
        return rpcError
    }

    override fun getResolvedBy(): List<Upstream> =
        resolvers.toList()

    override fun toString(): String {
        return "Quorum: Accept Any"
    }
}
