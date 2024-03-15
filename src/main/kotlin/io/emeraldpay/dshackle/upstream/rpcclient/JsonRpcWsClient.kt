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
package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainCallError
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.ethereum.WsConnectionPool
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import reactor.core.publisher.Mono

class JsonRpcWsClient(
    private val wsPool: WsConnectionPool,
) : ChainReader {

    override fun read(key: ChainRequest): Mono<ChainResponse> {
        val conn = wsPool.getConnection()
        if (!conn.isConnected) {
            return Mono.error(
                ChainException(
                    ChainResponse.NumberId(key.id),
                    ChainCallError(
                        RpcResponseError.CODE_UPSTREAM_CONNECTION_ERROR,
                        "WebSocket is not connected",
                    ),
                ),
            )
        }
        return conn.callRpc(key)
    }
}
