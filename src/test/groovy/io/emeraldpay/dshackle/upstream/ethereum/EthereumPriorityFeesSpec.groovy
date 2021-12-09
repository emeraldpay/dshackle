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

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ChainFees
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class EthereumPriorityFeesSpec extends Specification {

    def "Extract fee from EIP1559 tx"() {
        setup:
        // 13756007
        def block = new BlockJson().tap {
            it.baseFeePerGas = new Wei(104197355513)
        }
        // 0x5da50f35a51e56ecd4313417b1c30f9c088222f3f8763701effe14f3dd18b6cc
        def tx = new TransactionJson().tap {
            it.type = 2
            it.maxFeePerGas = Wei.ofUnits(999, Wei.Unit.GWEI)
            it.maxPriorityFeePerGas = Wei.ofUnits(5.0001, Wei.Unit.GWEI)
        }

        def fees = new EthereumPriorityFees(Stub(EthereumMultistream), Stub(EthereumReader), 10)
        when:
        def act = fees.extractFee(block, tx)
        then:
        act.priority == Wei.ofUnits(5.0001, Wei.Unit.GWEI)
        act.max == Wei.ofUnits(999, Wei.Unit.GWEI)
        act.base == new Wei(104197355513)
    }

    def "Extract fee from legacy tx"() {
        setup:
        // 13756007
        def block = new BlockJson().tap {
            it.baseFeePerGas = new Wei(104197355513)
        }
        // 0x1f507982bef0f11a8304287d41f228b5f1dda1114a446ee781c3d95ef4a7b891
        def tx = new TransactionJson().tap {
            it.type = 0
            // 109.564020111 Gwei
            it.gasPrice = Wei.from("0x198286458f")
        }

        def fees = new EthereumPriorityFees(Stub(EthereumMultistream), Stub(EthereumReader), 10)
        when:
        def act = fees.extractFee(block, tx)
        then:
        // as difference between base minimum and actually paid
        act.priority == Wei.ofUnits(5.366664598, Wei.Unit.GWEI)
        act.max == Wei.from("0x198286458f")
        act.base == new Wei(104197355513)
    }

    def "Calculates average fee"() {
        setup:
        def inputs = [
                new EthereumFees.EthereumFee(Wei.ofEthers(1), Wei.ofEthers(0.5), Wei.ofEthers(0.75), Wei.ofEthers(0.5)),
                new EthereumFees.EthereumFee(Wei.ofEthers(0.75), Wei.ofEthers(0.5), Wei.ofEthers(0.75), Wei.ofEthers(0.5)),
                new EthereumFees.EthereumFee(Wei.ofEthers(0.6), Wei.ofEthers(0.2), Wei.ofEthers(0.6), Wei.ofEthers(0.5)),
        ]
        def fees = new EthereumPriorityFees(Stub(EthereumMultistream), Stub(EthereumReader), 10)
        when:
        def act = Flux.fromIterable(inputs)
                .transform(fees.feeAggregation(ChainFees.Mode.AVG_LAST))
                .next().block(Duration.ofSeconds(1))
        then:
        act.priority == Wei.ofEthers(0.4) // 0.5 + 0.5 + 0.2
        act.paid == Wei.ofEthers(0.7) // 0.75 + 0.75 + 0.6
    }

    def "Estimate"() {
        setup:
        def block1 = new BlockJson().tap {
            it.baseFeePerGas = new Wei(92633661632)
            it.transactions = [
                    new TransactionRefJson(TransactionId.from("0x00000000fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                    new TransactionRefJson(TransactionId.from("0x11111111fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                    new TransactionRefJson(TransactionId.from("0x22222222fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
            ]
        }
        def block2 = new BlockJson().tap {
            it.baseFeePerGas = new Wei(104197355513)
            it.transactions = [
                    new TransactionRefJson(TransactionId.from("0x33333333fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                    new TransactionRefJson(TransactionId.from("0x44444444fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
                    new TransactionRefJson(TransactionId.from("0x55555555fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")),
            ]
        }
        def tx1 = new TransactionJson().tap {
            it.type = 2
            it.maxFeePerGas = Wei.ofUnits(150, Wei.Unit.GWEI)
            it.maxPriorityFeePerGas = Wei.ofUnits(3, Wei.Unit.GWEI)
        }
        def tx2 = new TransactionJson().tap {
            it.type = 2
            it.maxFeePerGas = Wei.ofUnits(200, Wei.Unit.GWEI)
            it.maxPriorityFeePerGas = Wei.ofUnits(6, Wei.Unit.GWEI)
        }

        def ups = Mock(EthereumMultistream) {
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> 13756007
            }
        }
        def reader = Mock(EthereumReader) {
            _ * it.blocksByHeightParsed() >> Mock(Reader) {
                1 * it.read(13756006) >> Mono.just(block1)
                1 * it.read(13756007) >> Mono.just(block2)
            }
            _ * it.txByHash() >> Mock(Reader) {
                1 * it.read(TransactionId.from("0x22222222fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")) >> Mono.just(tx1)
                1 * it.read(TransactionId.from("0x55555555fad596cad644b785a8a74f6580ceec9ae13c8aa174f819c0223b8c77")) >> Mono.just(tx2)
            }
        }
        def fees = new EthereumPriorityFees(ups, reader, 10)
        when:
        def act = fees.estimate(ChainFees.Mode.AVG_LAST, 2).block(Duration.ofSeconds(1))

        then:
        act.hasEthereumExtended()
        act.ethereumExtended.priority == "4500000000"
        act.ethereumExtended.max == "175000000000"
        act.ethereumExtended.expect == "102915508572"
    }
}
