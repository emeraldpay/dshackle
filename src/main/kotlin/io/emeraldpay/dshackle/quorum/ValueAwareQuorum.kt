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
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.etherjar.rpc.RpcException
import org.slf4j.LoggerFactory

abstract class ValueAwareQuorum<T>(
    val clazz: Class<T>
) : CallQuorum {

    private val log = LoggerFactory.getLogger(ValueAwareQuorum::class.java)
    private var rpcError: JsonRpcError? = null

    fun extractValue(response: ByteArray, clazz: Class<T>): T? {
        return Global.objectMapper.readValue(response.inputStream(), clazz)
    }

    override fun record(response: ByteArray, signature: ByteArray?, upstream: Upstream): Boolean {
        try {
            val value = extractValue(response, clazz)
            recordValue(response, value, signature, upstream)
        } catch (e: RpcException) {
            recordError(response, e.rpcMessage, signature, upstream)
        } catch (e: Exception) {
            recordError(response, e.message, signature, upstream)
        }
        return isResolved()
    }

    override fun record(error: JsonRpcException, signature: ByteArray?, upstream: Upstream) {
        this.rpcError = error.error
        recordError(null, error.error.message,  signature, upstream)
    }

    abstract fun recordValue(response: ByteArray, responseValue: T?, signature: ByteArray?, upstream: Upstream)

    abstract fun recordError(response: ByteArray?, errorMessage: String?, signature: ByteArray?, upstream: Upstream)

    override fun getError(): JsonRpcError? {
        return rpcError
    }
}
