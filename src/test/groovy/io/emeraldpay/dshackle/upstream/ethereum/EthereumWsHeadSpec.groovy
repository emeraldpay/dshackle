/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class EthereumWsHeadSpec extends Specification {

    def "Fetch block"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.transactions = [
                new TransactionRefJson(TransactionId.from("0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8")),
                new TransactionRefJson(TransactionId.from("0xebe8f22a55a9e26892a8545b93cbb2bfa4fd81c3184e50e5cf6276025bb42b93"))
        ]
        block.uncles = []
        block.totalDifficulty = BigInteger.ONE

        def headBlock = block.copy().tap {
            it.transactions = null
        }.with {
            Global.objectMapper.writeValueAsBytes(it)
        }

        def apiMock = TestingCommons.api()
        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], block)

        def ws = Mock(WsSubscriptions)

        def head = new EthereumWsHead(Chain.ETHEREUM, apiMock, ws)

        when:
        def act = head.listenNewHeads().blockFirst()

        then:
        act == BlockContainer.from(block)
        act.transactions.size() == 2
        act.transactions[0].toHexWithPrefix() == "0x29229361dc5aa1ec66c323dc7a299e2b61a8c8dd2a3522d41255ec10eca25dd8"
        act.transactions[1].toHexWithPrefix() == "0xebe8f22a55a9e26892a8545b93cbb2bfa4fd81c3184e50e5cf6276025bb42b93"

        1 * ws.subscribe("newHeads") >> Flux.fromIterable([
                headBlock
        ])
    }
}
