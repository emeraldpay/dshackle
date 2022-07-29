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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.AbstractChainFees
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuples
import java.util.function.Function

abstract class EthereumFees(
    upstreams: Multistream,
    private val reader: EthereumReader,
    heightLimit: Int,
) : AbstractChainFees<EthereumFees.EthereumFee, BlockJson<TransactionRefJson>, TransactionRefJson, TransactionJson>(heightLimit, upstreams, extractTx), ChainFees {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumFees::class.java)

        private val extractTx = { block: BlockJson<TransactionRefJson> ->
            block.transactions
        }
    }

    abstract fun extractFee(block: BlockJson<TransactionRefJson>, tx: TransactionJson): EthereumFee

    override fun readFeesAt(height: Long, selector: TxAt<BlockJson<TransactionRefJson>, TransactionRefJson>): Mono<EthereumFee> {
        return reader.blocksByHeightParsed().read(height)
            .flatMap { block ->
                Mono.justOrEmpty(selector.get(block))
                    .cast(TransactionRefJson::class.java)
                    .flatMap { reader.txByHash().read(it.hash) }
                    .map { tx -> extractFee(block, tx) }
            }
    }

    override fun feeAggregation(mode: ChainFees.Mode): Function<Flux<EthereumFee>, Mono<EthereumFee>> {
        if (mode == ChainFees.Mode.MIN_ALWAYS) {
            return Function { src ->
                src.reduce { a, b ->
                    EthereumFee(
                        a.max.coerceAtLeast(b.max),
                        a.priority.coerceAtLeast(b.priority),
                        a.paid.coerceAtLeast(b.paid),
                        Wei.ZERO
                    )
                }
            }
        }
        return Function { src ->
            src.map { Tuples.of(1, it) }
                .reduce { a, b ->
                    Tuples.of(a.t1 + b.t1, a.t2.plus(b.t2))
                }.map {
                    EthereumFee(it.t2.max / it.t1, it.t2.priority / it.t1, it.t2.paid / it.t1, it.t2.base / it.t1)
                }
        }
    }

    // ---

    data class EthereumFee(val max: Wei, val priority: Wei, val paid: Wei, val base: Wei) {
        fun plus(o: EthereumFee): EthereumFee {
            return EthereumFee(max + o.max, priority + o.priority, paid + o.paid, base + o.base)
        }
    }
}
