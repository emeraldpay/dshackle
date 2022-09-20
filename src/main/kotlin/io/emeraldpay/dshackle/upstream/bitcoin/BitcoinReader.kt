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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.cast

open class BitcoinReader(
    private val upstreams: BitcoinMultistream,
    head: Head,
    esploraClient: EsploraClient?
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinReader::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper
    private val mempool = CachingMempoolData(upstreams, head)
    private val selector = Selector.Builder()
        .withMatcher(Selector.CapabilityMatcher(Capability.RPC))
        .build()

    private val unspentReader: UnspentReader = if (esploraClient != null) {
        EsploraUnspentReader(esploraClient, head)
    } else if (upstreams.upstreams.any { it.isGrpc() && it.getCapabilities().contains(Capability.BALANCE) }) {
        RemoteUnspentReader(upstreams)
    } else {
        RpcUnspentReader(upstreams)
    }

    open fun getMempool(): CachingMempoolData {
        return mempool
    }

    open fun getBlock(hash: String): Mono<Map<String, Any>> {
        return castedRead(JsonRpcRequest("getblock", listOf(hash)), Map::class.java).cast()
    }

    open fun getBlock(height: Long): Mono<Map<String, Any>> {
        return castedRead(JsonRpcRequest("getblockhash", listOf(height)), String::class.java)
            .flatMap(this@BitcoinReader::getBlock)
    }

    open fun getTx(txid: String): Mono<Map<String, Any>> {
        return castedRead(JsonRpcRequest("getrawtransaction", listOf(txid, true)), Map::class.java).cast()
    }

    open fun listUnspent(address: Address): Mono<List<SimpleUnspent>> {
        return unspentReader.read(address)
    }

    override fun isRunning(): Boolean {
        return mempool.isRunning
    }

    override fun start() {
        mempool.start()
    }

    override fun stop() {
        mempool.stop()
    }

    fun <T> castedRead(req: JsonRpcRequest, clazz: Class<T>): Mono<T> {
        return upstreams.getDirectApi(selector).flatMap { api ->
            api.read(req)
                .flatMap(JsonRpcResponse::requireResult)
                .map {
                    objectMapper.readValue(it, clazz) as T
                }
        }
    }
}
