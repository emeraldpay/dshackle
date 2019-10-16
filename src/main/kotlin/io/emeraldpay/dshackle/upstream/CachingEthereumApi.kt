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
package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ethereum.EmptyEthereumHead
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumHead
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.ResponseJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Mono
import java.util.function.Function

open class CachingEthereumApi(
        private val objectMapper: ObjectMapper,
        private val cache: Reader<BlockHash, BlockJson<TransactionRefJson>>,
        private val head: EthereumHead
): EthereumApi(objectMapper) {

    companion object {
        @JvmStatic
        fun empty(): CachingEthereumApi {
            return CachingEthereumApi(ObjectMapper(), EmptyReader(), EmptyEthereumHead())
        }
    }

    override fun execute(id: Int, method: String, params: List<Any>): Mono<ByteArray> {
        return when (method) {
            "eth_blockNumber" ->
                head.getFlux().next()
                    .map { HexQuantity.from(it.number).toHex() }
                    .map(toJson(id))
            "eth_getBlockByHash" ->
                if (params.size == 2 && (params[1] == "false" || params[1] == false))
                    Mono.just(params[0])
                        .map { BlockHash.from(it as String) }
                        .flatMap(cache::read)
                        .map(toJson(id))
                else Mono.empty()
            else ->
                Mono.empty()
        }
    }

    fun toJson(id: Int): Function<Any, ByteArray> {
        return Function { data ->
            val resp = ResponseJson<Any, Int>()
            resp.id = id
            resp.result = data
            objectMapper.writer().writeValueAsBytes(resp)
        }
    }
}