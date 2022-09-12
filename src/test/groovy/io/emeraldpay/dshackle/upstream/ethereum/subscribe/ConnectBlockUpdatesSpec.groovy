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

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.Hex32
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class ConnectBlockUpdatesSpec extends Specification {

    def "Extracts updates"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(EthereumMultistream))
        def block = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")),
                    new TransactionRefJson(TransactionId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2"))
            ]
        })
        when:
        def act = connectBlockUpdates.extractUpdates(block)
                .collectList().block(Duration.ofSeconds(3))

        then:
        act.size() == 2
        with(act[0]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.transactionId == TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")
            it.type == ConnectBlockUpdates.UpdateType.NEW
        }
        with(act[1]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.transactionId == TxId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2")
            it.type == ConnectBlockUpdates.UpdateType.NEW
        }
    }

    def "Produce DROP updates for replaced block"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(EthereumMultistream))
        def block = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")),
                    new TransactionRefJson(TransactionId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2"))
            ]
        })
        when:
        def act = connectBlockUpdates.whenReplaced(block)
                .collectList().block(Duration.ofSeconds(3))

        then:
        act.size() == 2
        with(act[0]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.transactionId == TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")
            it.type == ConnectBlockUpdates.UpdateType.DROP
        }
        with(act[1]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.transactionId == TxId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2")
            it.type == ConnectBlockUpdates.UpdateType.DROP
        }
    }

    def "Gets prev version if available"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(EthereumMultistream))
        def block1 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")),
                    new TransactionRefJson(TransactionId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2"))
            ]
        })
        def block2 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")),
                    new TransactionRefJson(TransactionId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2"))
            ]
        })
        def block3 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xdb1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7")
            number = 13412872
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af6c88df9d65ccc")),
                    new TransactionRefJson(TransactionId.from("0xdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe25c241a64e7ce536f"))
            ]
        })

        when:
        def prev = connectBlockUpdates.findPrevious(block1)
        then:
        prev == null

        when:
        prev = connectBlockUpdates.findPrevious(block2)
        then:
        prev == null

        when:
        prev = connectBlockUpdates.findPrevious(block3)
        then:
        prev == null

        when:
        connectBlockUpdates.remember(block1)
        prev = connectBlockUpdates.findPrevious(block2)
        then:
        prev == block1

        when:
        prev = connectBlockUpdates.findPrevious(block3)
        then:
        prev == null
    }

    def "Marks old txes as dropped before producing a new version of same block"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(EthereumMultistream))
        def block1 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")),
                    new TransactionRefJson(TransactionId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2"))
            ]
        })
        def block2 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            transactions = [
                    new TransactionRefJson(TransactionId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")),
                    new TransactionRefJson(TransactionId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2"))
            ]
        })

        when:
        connectBlockUpdates.remember(block1)
        def act = connectBlockUpdates.extract(block2)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 4
        with(act[0]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.transactionId == TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")
            it.type == ConnectBlockUpdates.UpdateType.DROP
        }
        with(act[1]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.transactionId == TxId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2")
            it.type == ConnectBlockUpdates.UpdateType.DROP
        }
        with(act[2]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            it.transactionId == TxId.from("0x6c88df9d65ccc9351db65676c3581b29483e8dabb71c48ef7671c44b0d5568af")
            it.type == ConnectBlockUpdates.UpdateType.NEW
        }
        with(act[3]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            it.transactionId == TxId.from("0x5c241a64e7ce536fdb6b8912091151f18a23dd71cc76a48a4b7d453e339efbe2")
            it.type == ConnectBlockUpdates.UpdateType.NEW
        }
    }

    def "Keeps connection"() {
        setup:
        def head = Mock(Head) {
            1 * getFlux() >> Flux.never()
        }
        def up = Mock(EthereumMultistream) {
            1 * getHead(Selector.empty) >> head
        }
        def connectBlockUpdates = new ConnectBlockUpdates(up)

        when:
        def a1 = connectBlockUpdates.connect()
        def a2 = connectBlockUpdates.connect()

        then:
        a1 == a2
    }
}
