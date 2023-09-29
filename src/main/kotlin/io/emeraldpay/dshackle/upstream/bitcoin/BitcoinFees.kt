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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.upstream.AbstractChainFees
import io.emeraldpay.dshackle.upstream.ChainFees
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.util.function.Function

class BitcoinFees(
    upstreams: BitcoinMultistream,
    private val reader: BitcoinReader,
    heightLimit: Int,
) : AbstractChainFees<BitcoinFees.TxFee, Map<String, Any>, String, Map<String, Any>>(heightLimit, upstreams, extractTx), ChainFees {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinFees::class.java)

        private val extractTx = { block: Map<String, Any> ->
            block["tx"]
                ?.let { it as List<String> }
                // drop first tx which is a miner reward
                ?.let { if (it.isEmpty()) it else it.drop(1) }
                ?: emptyList()
        }
    }

    fun calculateFee(tx: Map<String, Any>): Mono<Long> {
        val outAmount = extractVOuts(tx).reduce { acc, l -> (acc ?: 0L) + (l ?: 0L) } ?: 0
        val vinsData = tx.get("vin")?.let { it as List<Map<String, Any>> } ?: return Mono.empty()
        return Flux.fromIterable(vinsData)
            .flatMap {
                val txid = it["txid"] as String?
                val vout = (it["vout"] as Number?)?.toInt()
                if (txid == null || vout == null) {
                    Mono.empty()
                } else {
                    getTxOutAmount(txid, vout)
                }
            }
            .reduce { t, u -> t + u }
            .map { inAmount ->
                (inAmount - outAmount).coerceAtLeast(0)
            }
    }

    fun extractSize(tx: Map<String, Any>): Int {
        val size: Number = if (tx.containsKey("vsize")) {
            tx["vsize"] as Number
        } else if (tx.containsKey("size")) {
            tx["size"] as Number
        } else {
            0
        }
        return size.toInt()
    }

    fun getTxOutAmount(txid: String, vout: Int): Mono<Long> {
        return reader.getTx(txid)
            .switchIfEmpty(
                Mono.fromCallable { log.warn("No tx $txid") }
                    .then(Mono.empty()),
            )
            .flatMap {
                extractVOuts(it).let {
                    if (vout < it.size) {
                        Mono.justOrEmpty(it[vout])
                    } else {
                        Mono.empty()
                    }
                }
            }
    }

    fun extractVOuts(tx: Map<String, Any>): List<Long?> {
        val voutsData = tx.get("vout")?.let { it as List<Map<String, Any>> } ?: return emptyList()
        return voutsData.map {
            val amount = it["value"] ?: return@map null
            BigDecimal(amount.toString())
                .multiply(BitcoinConst.COIN_DEC)
                .longValueExact()
        }
    }

    override fun readFeesAt(height: Long, selector: TxAt<Map<String, Any>, String>): Mono<TxFee> {
        return reader.getBlock(height)
            .flatMap { block ->
                Mono.justOrEmpty(selector.get(block))
                    .flatMap { txid -> reader.getTx(txid!!) }
                    .flatMap { tx ->
                        calculateFee(tx)
                            .map { fee ->
                                TxFee(1, fee * 1024 / extractSize(tx))
                            }
                    }
            }
    }

    override fun feeAggregation(mode: ChainFees.Mode): Function<Flux<TxFee>, Mono<TxFee>> {
        if (mode == ChainFees.Mode.MIN_ALWAYS) {
            return Function { src ->
                src.reduce { a, b ->
                    if (a.fee > b.fee) a else b
                }
            }
        }
        return Function { src ->
            src.reduce { a, b ->
                TxFee(a.count + b.count, a.fee + b.fee)
            }
        }
    }

    override fun getResponseBuilder(): Function<TxFee, BlockchainOuterClass.EstimateFeeResponse> {
        return Function {
            val fee = (it.fee / it.count).coerceAtLeast(1)
            BlockchainOuterClass.EstimateFeeResponse.newBuilder()
                .setBitcoinStd(
                    BlockchainOuterClass.BitcoinStdFees.newBuilder()
                        .setSatPerKb(fee),
                )
                .build()
        }
    }

    // -------

    data class TxFee(val count: Int, val fee: Long)
}
