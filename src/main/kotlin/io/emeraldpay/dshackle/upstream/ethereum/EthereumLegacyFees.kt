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
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import java.util.function.Function

class EthereumLegacyFees(upstreams: EthereumMultistream, reader: EthereumCachingReader, heightLimit: Int) :
    EthereumFees(upstreams, reader, heightLimit) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumLegacyFees::class.java)
    }

    private val toGrpc: Function<EthereumFee, BlockchainOuterClass.EstimateFeeResponse> = Function {
        BlockchainOuterClass.EstimateFeeResponse.newBuilder()
            .setEthereumStd(
                BlockchainOuterClass.EthereumStdFees.newBuilder()
                    .setFee(it.paid.amount.toString())
            )
            .build()
    }

    override fun extractFee(block: BlockJson<TransactionRefJson>, tx: TransactionJson): EthereumFee {
        return EthereumFee(tx.gasPrice, tx.gasPrice, tx.gasPrice, Wei.ZERO)
    }

    override fun getResponseBuilder(): Function<EthereumFee, BlockchainOuterClass.EstimateFeeResponse> {
        return toGrpc
    }
}
