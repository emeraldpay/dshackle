/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.bitcoin.data.RpcUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class RpcUnspentReader(
    private val upstreams: BitcoinMultistream
) : UnspentReader {

    companion object {
        private val log = LoggerFactory.getLogger(RpcUnspentReader::class.java)

        @JvmStatic
        val selector = Selector.Builder()
            // for BITCOIN balance we need an upstream can provide a balance
            .withMatcher(Selector.CapabilityMatcher(Capability.BALANCE))
            // but since we make an RPC call we need this capability as well (as opposed to RemoteUnspentReader)
            .withMatcher(Selector.CapabilityMatcher(Capability.RPC))
            .build()
    }

    private val convert: (T: RpcUnspent) -> SimpleUnspent = { base ->
        SimpleUnspent(
            base.txid,
            base.vout,
            base.amount,
        )
    }

    override fun read(key: Address): Mono<List<SimpleUnspent>> {
        // docs: https://developer.bitcoin.org/reference/rpc/listunspent.html
        //
        val address = key.toString()
        return upstreams.read(DshackleRequest(1, "listunspent", listOf(1, 9999999, listOf(address)), matcher = selector))
            .flatMap(DshackleResponse::requireResult)
            .map {
                Global.objectMapper.readerFor(RpcUnspent::class.java).readValues<RpcUnspent>(it).readAll()
            }
            .map {
                it.filter {
                    it.address == address
                }.map(convert)
            }
            .switchIfEmpty(Mono.error(SilentException.DataUnavailable("BALANCE")))
    }
}
