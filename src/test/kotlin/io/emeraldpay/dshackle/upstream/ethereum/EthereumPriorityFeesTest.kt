/**
 * Copyright (c) 2025 EmeraldPay Ltd
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

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

class EthereumPriorityFeesTest : ShouldSpec({

    should("Extract fee from EIP1559 tx") {
        // 13756007
        val block = BlockJson<TransactionRefJson>().apply {
            baseFeePerGas = Wei(104197355513)
        }
        // 0x5da50f35a51e56ecd4313417b1c30f9c088222f3f8763701effe14f3dd18b6cc
        val tx = TransactionJson().apply {
            type = 2
            maxFeePerGas = Wei.ofUnits(999.0, Wei.Unit.GWEI)
            maxPriorityFeePerGas = Wei.ofUnits(5.0001, Wei.Unit.GWEI)
        }

        val fees = EthereumPriorityFees(mockk<EthereumMultistream>(), mockk<DataReaders>(), 10)
        val act = fees.extractFee(block, tx)

        act.priority shouldBe Wei.ofUnits(5.0001, Wei.Unit.GWEI)
        act.max shouldBe Wei.ofUnits(999.0, Wei.Unit.GWEI)
        act.base shouldBe Wei(104197355513)
    }

    should("Extract fee from legacy tx") {
        // 13756007
        val block = BlockJson<TransactionRefJson>().apply {
            baseFeePerGas = Wei(104197355513)
        }
        // 0x1f507982bef0f11a8304287d41f228b5f1dda1114a446ee781c3d95ef4a7b891
        val tx = TransactionJson().apply {
            type = 0
            // 109.564020111 Gwei
            gasPrice = Wei.fromHex("0x198286458f")
        }

        val fees = EthereumPriorityFees(mockk<EthereumMultistream>(), mockk<DataReaders>(), 10)
        val act = fees.extractFee(block, tx)

        // as difference between base minimum and actually paid
        act.priority shouldBe Wei.ofUnits(5.366664598, Wei.Unit.GWEI)
        act.max shouldBe Wei.fromHex("0x198286458f")
        act.base shouldBe Wei(104197355513)
    }

    should("Calculates average fee") {
        val inputs = listOf(
            EthereumFees.EthereumFee(Wei.ofEthers(1.0), Wei.ofEthers(0.5), Wei.ofEthers(0.75), Wei.ofEthers(0.5)),
            EthereumFees.EthereumFee(Wei.ofEthers(0.75), Wei.ofEthers(0.5), Wei.ofEthers(0.75), Wei.ofEthers(0.5)),
            EthereumFees.EthereumFee(Wei.ofEthers(0.6), Wei.ofEthers(0.2), Wei.ofEthers(0.6), Wei.ofEthers(0.5)),
        )
        val fees = EthereumPriorityFees(mockk<EthereumMultistream>(), mockk<DataReaders>(), 10)

        val act = Flux.fromIterable(inputs)
            .transform(fees.feeAggregation(ChainFees.Mode.AVG_LAST))
            .next().block(Duration.ofSeconds(1))!!

        act.priority shouldBe Wei.ofEthers(0.4) // 0.5 + 0.5 + 0.2
        act.paid shouldBe Wei.ofEthers(0.7) // 0.75 + 0.75 + 0.6
    }

    should("Estimate") {
        val block1 = BlockJson<TransactionRefJson>().apply {
            baseFeePerGas = Wei(92633661632)
            transactions = listOf(
                TransactionRefJson(TransactionId.from("0x00000000fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                TransactionRefJson(TransactionId.from("0x11111111fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                TransactionRefJson(TransactionId.from("0x22222222fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
            )
        }
        val block2 = BlockJson<TransactionRefJson>().apply {
            baseFeePerGas = Wei(104197355513)
            transactions = listOf(
                TransactionRefJson(TransactionId.from("0x33333333fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                TransactionRefJson(TransactionId.from("0x44444444fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                TransactionRefJson(TransactionId.from("0x55555555fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
            )
        }
        val tx1 = TransactionJson().apply {
            type = 2
            maxFeePerGas = Wei.ofUnits(150.0, Wei.Unit.GWEI)
            maxPriorityFeePerGas = Wei.ofUnits(3.0, Wei.Unit.GWEI)
        }
        val tx2 = TransactionJson().apply {
            type = 2
            maxFeePerGas = Wei.ofUnits(200.0, Wei.Unit.GWEI)
            maxPriorityFeePerGas = Wei.ofUnits(6.0, Wei.Unit.GWEI)
        }

        val ups = mockk<EthereumMultistream> {
            every { getHead() } returns mockk<Head> {
                every { getCurrentHeight() } returns 13756007
            }
        }
        val reader = mockk<DataReaders> {
            every { blocksByHeightParsed } returns mockk<Reader<Long, BlockJson<TransactionRefJson>>> {
                every { read(13756006) } returns Mono.just(block1)
                every { read(13756007) } returns Mono.just(block2)
            }
            every { txReaderParsed } returns mockk<Reader<TransactionId, TransactionJson>> {
                every { read(TransactionId.from("0x22222222fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")) } returns Mono.just(tx1)
                every { read(TransactionId.from("0x55555555fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")) } returns Mono.just(tx2)
            }
        }
        val fees = EthereumPriorityFees(ups, reader, 10)

        val act = fees.estimate(ChainFees.Mode.AVG_LAST, 2).block(Duration.ofSeconds(1))!!

        act.hasEthereumExtended() shouldBe true
        act.ethereumExtended.priority shouldBe "4500000000"
        act.ethereumExtended.max shouldBe "175000000000"
        act.ethereumExtended.expect shouldBe "102915508572"
    }
})
