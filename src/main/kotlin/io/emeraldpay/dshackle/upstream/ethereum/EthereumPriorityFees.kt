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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import java.util.function.Function

class EthereumPriorityFees(upstreams: EthereumMultistream, reader: EthereumReader, heightLimit: Int) :
    EthereumFees(upstreams, reader, heightLimit) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumPriorityFees::class.java)
    }

    private val toGrpc: Function<EthereumFee, BlockchainOuterClass.EstimateFeeResponse> =
        Function {
            BlockchainOuterClass.EstimateFeeResponse.newBuilder()
                .setEthereumExtended(
                    BlockchainOuterClass.EthereumExtFees.newBuilder()
                        .setMax(it.max.amount.toString())
                        .setPriority(it.priority.amount.toString())
                        .setExpect(it.paid.amount.toString())
                )
                .build()
        }

    override fun extractFee(block: BlockJson<TransactionRefJson>, tx: TransactionJson): EthereumFee {
        val baseFee = block.baseFeePerGas ?: Wei.ZERO
        if (tx.type == 2) {
            // an EIP-1559 Transaction provides Max and Priority fee
            val paid = (baseFee + tx.maxPriorityFeePerGas).coerceAtMost(tx.maxFeePerGas)
            return EthereumFee(tx.maxFeePerGas, tx.maxPriorityFeePerGas, paid, baseFee)
        }
        return EthereumFee(tx.gasPrice, (tx.gasPrice - baseFee).coerceAtLeast(Wei.ZERO), tx.gasPrice, baseFee)
    }

    override fun getResponseBuilder(): Function<EthereumFee, BlockchainOuterClass.EstimateFeeResponse> {
        return toGrpc
    }
}
