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

interface CallQuorum {
    fun isResolved(): Boolean
    fun isFailed(): Boolean

    fun record(
        response: ChainResponse,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean

    fun record(
        error: ChainException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    )
    fun getSignature(): ResponseSigner.Signature?

    fun getResponse(): ChainResponse?
    fun getError(): ChainCallError?
    fun getResolvedBy(): Collection<Upstream>
}
