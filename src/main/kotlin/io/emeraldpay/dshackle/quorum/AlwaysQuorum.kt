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

import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner

open class AlwaysQuorum : CallQuorum {

    private var resolved = false
    private var result: ChainResponse? = null
    private var rpcError: ChainCallError? = null
    private var sig: ResponseSigner.Signature? = null
    private val resolvers = ArrayList<Upstream>()

    override fun isResolved(): Boolean {
        return resolved
    }

    override fun isFailed(): Boolean {
        return rpcError != null
    }

    override fun getSignature(): ResponseSigner.Signature? {
        return sig
    }

    override fun record(
        response: ChainResponse,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean {
        result = response
        resolved = true
        sig = signature
        resolvers.add(upstream)
        return true
    }

    override fun record(
        error: ChainException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        this.rpcError = error.error
        sig = signature
        resolvers.add(upstream)
    }

    override fun getResponse(): ChainResponse? {
        return result
    }

    override fun getError(): ChainCallError? {
        return rpcError
    }

    override fun getResolvedBy(): List<Upstream> = resolvers

    override fun toString(): String {
        return "Quorum: Accept Any"
    }
}
