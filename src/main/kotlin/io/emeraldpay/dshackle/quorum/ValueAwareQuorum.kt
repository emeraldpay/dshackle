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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import org.slf4j.LoggerFactory

abstract class ValueAwareQuorum<T>(
    val clazz: Class<T>,
) : CallQuorum {

    private val log = LoggerFactory.getLogger(ValueAwareQuorum::class.java)
    private var rpcError: ChainCallError? = null
    protected val resolvers = ArrayList<Upstream>()

    fun extractValue(response: ByteArray, clazz: Class<T>): T? {
        return Global.objectMapper.readValue(response.inputStream(), clazz)
    }

    override fun record(
        response: ChainResponse,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean {
        if (response.hasStream()) {
            throw IllegalStateException("ValueAwareQuorum works with value, response must not have stream")
        }
        try {
            val value = extractValue(response.getResult(), clazz)
            recordValue(response, value, signature, upstream)
            resolvers.add(upstream)
        } catch (e: RpcException) {
            recordError(e.rpcMessage, signature, upstream)
        } catch (e: Exception) {
            recordError(e.message, signature, upstream)
        }
        return isResolved()
    }

    override fun record(
        error: ChainException,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ) {
        this.rpcError = error.error
        recordError(error.error.message, signature, upstream)
    }

    abstract fun recordValue(
        response: ChainResponse,
        responseValue: T?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    )

    abstract fun recordError(
        errorMessage: String?,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    )

    override fun getError(): ChainCallError? {
        return rpcError
    }

    override fun getResolvedBy(): Collection<Upstream> = resolvers
}
