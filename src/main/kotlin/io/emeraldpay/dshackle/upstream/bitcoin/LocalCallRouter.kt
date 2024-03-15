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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.bitcoinj.core.Address
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
    private val reader: BitcoinReader,
) : ChainReader {

    companion object {
        private val log = LoggerFactory.getLogger(LocalCallRouter::class.java)
    }

    override fun read(key: ChainRequest): Mono<ChainResponse> {
        if (methods.isHardcoded(key.method)) {
            return Mono.just(methods.executeHardcoded(key.method))
                .map { ChainResponse(it, null) }
        }
        if (!methods.isCallable(key.method)) {
            return Mono.error(RpcException(RpcResponseError.CODE_METHOD_NOT_EXIST, "Unsupported method"))
        }
        if (key.method == "listunspent") {
            return processUnspentRequest(key)
        }
        return Mono.empty()
    }

    /**
     *
     */
    fun processUnspentRequest(key: ChainRequest): Mono<ChainResponse> {
        if (key.params is ListParams) {
            if (key.params.list.size < 3) {
                return Mono.error(SilentException("Invalid call to unspent. Address is missing"))
            }
            val addresses = key.params.list[2]
            if (addresses is List<*> && addresses.size > 0) {
                val address = addresses[0].toString().let { Address.fromString(null, it) }
                return reader.listUnspent(address).map {
                    val rpc = it.map(convertUnspent(address))
                    val json = Global.objectMapper.writeValueAsBytes(rpc)
                    ChainResponse.ok(json, ChainResponse.NumberId(key.id))
                }
            }
        }
        return Mono.error(SilentException("Invalid call to unspent"))
    }

    fun convertUnspent(address: Address): (SimpleUnspent) -> RpcUnspent {
        return { base ->
            RpcUnspent(
                base.txid,
                base.vout,
                address.toString(),
                base.value,
            )
        }
    }
}
