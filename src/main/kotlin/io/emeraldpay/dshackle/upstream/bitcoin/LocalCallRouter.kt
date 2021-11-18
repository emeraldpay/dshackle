/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Reader for JSON RPC requests. Verifies if the method is allowed, transforms if necessary, and calls EthereumReader for data.
 * It provides data only if it's available through the router (cached, head, etc).
 * If data is not available locally then it returns `empty`; at this case the caller should call the remote node for actual data.
 *
 * @see BitcoinReader
 */
class LocalCallRouter(
    private val methods: CallMethods,
) : Reader<JsonRpcRequest, JsonRpcResponse> {

    companion object {
        private val log = LoggerFactory.getLogger(LocalCallRouter::class.java)
    }

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        if (methods.isHardcoded(key.method)) {
            return Mono.just(methods.executeHardcoded(key.method))
                .map { JsonRpcResponse(it, null) }
        }
        if (!methods.isAllowed(key.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        return Mono.empty()
    }

}