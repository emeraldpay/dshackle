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
package io.emeraldpay.dshackle.reader

import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Reader that requests data through upstream RPC using provided JSON RPC request builder
 */
class RpcReader<T>(
    private val up: Multistream,
    private val paramsBuilder: (T) -> JsonRpcRequest
) : Reader<T, ByteArray> {

    companion object {
        private val log = LoggerFactory.getLogger(RpcReader::class.java)

        /**
         * Common reader that just passes key as a parameter with the specified method. The key must be serializable to JSON.
         * @param method RPC method to use
         */
        fun <T> basicRequest(up: Multistream, method: String): RpcReader<T> {
            return RpcReader(up) { key ->
                JsonRpcRequest(method, listOf(key))
            }
        }
    }

    override fun read(key: T): Mono<ByteArray> {
        return up.getDirectApi(Selector.empty)
            .flatMap { rdr ->
                rdr.read(paramsBuilder(key)).flatMap {
                    it.requireResult()
                }
            }
    }
}
