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
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class ProduceLogsSpec extends Specification {

    def "Produce added as nothing with no logs"() {
        setup:
        String receipt = '{\n' +
                '    "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '    "blockNumber": "0x1",\n' +
                '    "from": "0x5e78dd1e81ecdf078e029117eca98eaa71f46bdb",\n' +
                '    "logs": [\n' +
                '    ],\n' +
                '    "transactionHash": "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af",\n' +
                '    "transactionIndex": "0x0"\n' +
                '  }'

        def receipts = Mock(Reader) {
            1 * it.read(TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")) >>
                    Mono.just(new EthereumDirectReader.Result<>(receipt.getBytes(), null))
        }
        def producer = new ProduceLogs(receipts, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af"),
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 0
    }

    def "Produce added as nothing with null receipt"() {
        setup:
        String receipt = 'null'

        def receipts = Mock(Reader) {
            1 * it.read(TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")) >>
                    Mono.just(new EthereumDirectReader.Result<>(receipt.getBytes(), null))
        }
        def producer = new ProduceLogs(receipts, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af"),
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 0
    }

    def "Produce added with single log"() {
        setup:
        String receipt = '{\n' +
                '    "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '    "blockNumber": "0x1",\n' +
                '    "from": "0x5e78dd1e81ecdf078e029117eca98eaa71f46bdb",\n' +
                '    "logs": [\n' +
                '      {\n' +
                '        "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005e78dd1e81ecdf078e029117eca98eaa71f46bdb",\n' +
                '          "0x00000000000000000000000099897cb0e667d354b920fc38a40a5100b2a01566"\n' +
                '        ],\n' +
                '        "data": "0x00000000000000000000000000000000000000000000000000000007505d91f0",\n' +
                '        "blockNumber": "0xc7f3b4",\n' +
                '        "transactionHash": "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af",\n' +
                '        "transactionIndex": "0x0",\n' +
                '        "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '        "logIndex": "0x0",\n' +
                '        "removed": false\n' +
                '      }' +
                '    ],\n' +
                '    "transactionHash": "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af",\n' +
                '    "transactionIndex": "0x0"\n' +
                '  }'

        def receipts = Mock(Reader) {
            1 * it.read(TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")) >>
                    Mono.just(new EthereumDirectReader.Result<>(receipt.getBytes(), null))
        }
        def producer = new ProduceLogs(receipts, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af"),
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 1
        with(act[0]) {
            it.transactionHash.toHex() == "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af"
        }
    }

    def "Produce added with no data"() {
        setup:
        String receipt = '{\n' +
                '    "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '    "blockNumber": "0x1",\n' +
                '    "from": "0x5e78dd1e81ecdf078e029117eca98eaa71f46bdb",\n' +
                '    "logs": [\n' +
                '      {\n' +
                '        "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"\n' +
                '        ],\n' +
                '        "blockNumber": "0xc7f3b4",\n' +
                '        "transactionHash": "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af",\n' +
                '        "transactionIndex": "0x0",\n' +
                '        "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '        "logIndex": "0x0",\n' +
                '        "removed": false\n' +
                '      }' +
                '    ],\n' +
                '    "transactionHash": "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af",\n' +
                '    "transactionIndex": "0x0"\n' +
                '  }'

        def receipts = Mock(Reader) {
            1 * it.read(TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")) >>
                    Mono.just(new EthereumDirectReader.Result<>(receipt.getBytes(), null))
        }
        def producer = new ProduceLogs(receipts, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af"),
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 1
        with(act[0]) {
            it.transactionHash.toHex() == "0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af"
            // Geth actually renders it as null, so this check may be wrong
            it.data != null && it.data.size == 0
        }
    }

    def "Produce added with multiple logs"() {
        setup:
        String receipt = '{\n' +
                '    "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '    "blockNumber": "0x1",\n' +
                '    "from": "0x5e78dd1e81ecdf078e029117eca98eaa71f46bdb",\n' +
                '    "logs": [\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x00000000000000000000000075b8c48bdb04d426aed57b36bb835ad2dc321c30"\n' +
                '        ],\n' +
                '        "data": "0x0000000000000000000000000000000000000000000000013e7ec767db370000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb4",\n' +
                '        "removed": false\n' +
                '      },\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x0000000000000000000000000000000000000000000000000000000000000000"\n' +
                '        ],\n' +
                '        "data": "0x00000000000000000000000000000000000000000000000023636b7d513f0000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb5",\n' +
                '        "removed": false\n' +
                '      },\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x0000000000000000000000001b46b72c5280f30fbe8a958b4f3c348fd0fd2e55"\n' +
                '        ],\n' +
                '        "data": "0x00000000000000000000000000000000000000000000011316d590258fba0000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb6",\n' +
                '        "removed": false\n' +
                '      },\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x0000000000000000000000001b46b72c5280f30fbe8a958b4f3c348fd0fd2e55"\n' +
                '        ],\n' +
                '        "data": "0x0000000000000000000000000000000000000000000014188a101e403a500000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb7",\n' +
                '        "removed": false\n' +
                '      },\n' +
                '      {\n' +
                '        "address": "0x1b46b72c5280f30fbe8a958b4f3c348fd0fd2e55",\n' +
                '        "topics": [\n' +
                '          "0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x0000000000000000000000000000000000000000000000000000000000000000"\n' +
                '        ],\n' +
                '        "data": "0x00000000000000000000000000000000000000000000011478b7c30abc300000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb8",\n' +
                '        "removed": false\n' +
                '      }' +
                '    ],\n' +
                '    "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '    "transactionIndex": "0x57"\n' +
                '  }'

        def receipts = Mock(Reader) {
            1 * it.read(TxId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec")) >>
                    Mono.just(new EthereumDirectReader.Result<>(receipt.getBytes(), null))
        }
        def producer = new ProduceLogs(receipts, Chain.ETHEREUM__MAINNET)
        def update = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                TxId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
                "upstream"
        )
        when:
        def act = producer.produceAdded(update)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 5
        act*.transactionHash.every { it.toHex() == "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec" }
        act*.logIndex == [180, 181, 182, 183, 184]
        act*.removed.every { !it }
    }

    def "Produce removed"() {
        setup:
        String receipt = '{\n' +
                '    "blockHash": "0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da",\n' +
                '    "blockNumber": "0x1",\n' +
                '    "from": "0x5e78dd1e81ecdf078e029117eca98eaa71f46bdb",\n' +
                '    "logs": [\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x00000000000000000000000075b8c48bdb04d426aed57b36bb835ad2dc321c30"\n' +
                '        ],\n' +
                '        "data": "0x0000000000000000000000000000000000000000000000013e7ec767db370000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb4",\n' +
                '        "removed": false\n' +
                '      },\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x0000000000000000000000000000000000000000000000000000000000000000"\n' +
                '        ],\n' +
                '        "data": "0x00000000000000000000000000000000000000000000000023636b7d513f0000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb5",\n' +
                '        "removed": false\n' +
                '      },\n' +
                '      {\n' +
                '        "address": "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",\n' +
                '        "topics": [\n' +
                '          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",\n' +
                '          "0x0000000000000000000000005c6006105b1b777a13d58d96393ad9e556882025",\n' +
                '          "0x0000000000000000000000001b46b72c5280f30fbe8a958b4f3c348fd0fd2e55"\n' +
                '        ],\n' +
                '        "data": "0x00000000000000000000000000000000000000000000011316d590258fba0000",\n' +
                '        "blockNumber": "0xccc493",\n' +
                '        "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '        "transactionIndex": "0x57",\n' +
                '        "blockHash": "0x7e2661ac0e2f34dd2d6f449eea45aeec8470a0948af9daa33e684226640d819c",\n' +
                '        "logIndex": "0xb6",\n' +
                '        "removed": false\n' +
                '      }\n' +
                '    ],\n' +
                '    "transactionHash": "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec",\n' +
                '    "transactionIndex": "0x57"\n' +
                '  }'

        def receipts = Mock(Reader) {
            1 * it.read(TxId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec")) >>
                    Mono.just(new EthereumDirectReader.Result<>(receipt.getBytes(), null))
        }
        def producer = new ProduceLogs(receipts, Chain.ETHEREUM__MAINNET)
        def update1 = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.NEW,
                TxId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
                "upstream"
        )
        def update2 = new ConnectBlockUpdates.Update(
                BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da"),
                13412871,
                ConnectBlockUpdates.UpdateType.DROP,
                TxId.from("0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec"),
                "upstream"
        )
        when:
        // first need to produce them as added, because that's when it remembers logs to "remove"
        producer.produceAdded(update1)
                .collectList().block(Duration.ofSeconds(1))
        def act = producer.produceRemoved(update2)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 3
        act*.transactionHash.every { it.toHex() == "0xb5e554178a94fd993111f2ae64cb708cb0899d7b5182024e70d5c468164a8bec" }
        act*.logIndex == [180, 181, 182]
        act*.removed.every { it }
    }
}
