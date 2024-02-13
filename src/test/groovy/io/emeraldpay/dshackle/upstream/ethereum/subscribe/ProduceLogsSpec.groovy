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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumDirectReader
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionLogJson
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class ProduceLogsSpec extends Specification {

    def logs = [new TransactionLogJson().tap {
        blockHash = BlockHash.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
        address = Address.from("0xe0aadb0a012dbcdc529c4c743d3e0385a0b54d3d")
        blockNumber = 1
        logIndex = 1
        data = HexData.empty()
        transactionIndex = 1
        topics = []
        transactionHash = TransactionId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec")
    }]

    def "Produce added as nothing with no logs"() {
        setup:
        def logs = Mock(Reader) {
            1 * it.read(BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")) >>
                    Mono.just(new EthereumDirectReader.Result<>([], null))
        }
        def producer = new ProduceLogs(logs, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                1,
                ConnectBlockUpdates.UpdateType.NEW,
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 0
    }

    def "Produce added with logs"() {
        setup:

        def logs = Mock(Reader) {
            1 * it.read(BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")) >>
                    Mono.just(new EthereumDirectReader.Result<>(logs, null))
        }
        def producer = new ProduceLogs(logs, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                1,
                ConnectBlockUpdates.UpdateType.NEW,
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 1
        act*.address.every { it.toHex() == "0xe0aadb0a012dbcdc529c4c743d3e0385a0b54d3d" }
        act*.removed.every { !it }
    }

    def "Produce removed"() {
        setup:
        def logs = Mock(Reader) {
            1 * it.read(BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")) >>
                    Mono.just(new EthereumDirectReader.Result<>(logs, null))
        }
        def producer = new ProduceLogs(logs, Chain.ETHEREUM__MAINNET)
        def update1 = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                "upstream"
        )
        def update2 = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.DROP,
                "upstream"
        )
        when:
        // first need to produce them as added, because that's when it remembers logs to "remove"
        producer.produceAdded(update1)
                .collectList().block(Duration.ofSeconds(1))
        def act = producer.produceRemoved(update2)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 1
        act*.transactionHash.every { it.toHex() == "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec" }
        act*.logIndex == [1]
        act*.removed.every { it }
    }
}
