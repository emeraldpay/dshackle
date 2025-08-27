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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.monitoring.record.RequestRecord
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Function

open class DefaultEthereumHead(
    private val blockchain: Chain,
) : AbstractHead(),
    Head {
    companion object {
        private val log = LoggerFactory.getLogger(DefaultEthereumHead::class.java)
    }

    fun getLatestBlock(api: StandardRpcReader): Mono<BlockContainer> =
        getBlockNumber(api)
            .transform(getBlockDetails(api))
            .onErrorResume { err ->
                log.debug("Failed to fetch latest block: ${err.message}")
                Mono.empty()
            }

    private fun getBlockNumber(api: StandardRpcReader): Mono<HexQuantity> {
        val request = JsonRpcRequest("eth_blockNumber", emptyList())
        return api
            .read(request)
            .subscribeOn(EthereumRpcHead.scheduler)
            .timeout(Defaults.timeout, Mono.error(SilentException.Timeout("Block number not received")))
            .contextWrite(Global.monitoring.ingress.withBlockchain(blockchain))
            .contextWrite(Global.monitoring.ingress.withRequest(request))
            .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.INTERNAL))
            .flatMap {
                if (it.error != null) {
                    Mono.error(it.error.asException(null))
                } else {
                    val value = it.resultAsProcessedString
                    Mono.just(HexQuantity.from(value))
                }
            }
    }

    private fun getBlockDetails(api: StandardRpcReader): Function<Mono<HexQuantity>, Mono<BlockContainer>> =
        Function { numberResponse ->
            numberResponse
                .flatMap { number ->
                    // fetching by Block Height here, critical to use the same upstream as in previous call,
                    // b/c different upstreams may have different blocks on the same height
                    val request = JsonRpcRequest("eth_getBlockByNumber", listOf(number.toHex(), false))
                    api
                        .read(request)
                        .subscribeOn(EthereumRpcHead.scheduler)
                        .timeout(Defaults.timeout, Mono.error(SilentException.Timeout("Block data not received")))
                        .contextWrite(Global.monitoring.ingress.withBlockchain(blockchain))
                        .contextWrite(Global.monitoring.ingress.withRequest(request))
                        .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.INTERNAL))
                }.map {
                    BlockContainer.fromEthereumJson(it.resultOrEmpty)
                }
        }
}
