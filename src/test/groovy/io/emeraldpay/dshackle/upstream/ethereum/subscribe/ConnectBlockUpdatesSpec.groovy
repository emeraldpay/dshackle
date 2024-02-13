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
import io.emeraldpay.dshackle.upstream.generic.GenericMultistream
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class ConnectBlockUpdatesSpec extends Specification {

    BlockHash parent = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")

    def "Extracts updates"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(GenericMultistream), Schedulers.boundedElastic())
        def block = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            parentHash = parent
            transactions = []
        })
        when:
        def act = connectBlockUpdates.extractUpdates(block)
                .collectList().block(Duration.ofSeconds(3))

        then:
        act.size() == 1
        with(act[0]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.type == ConnectBlockUpdates.UpdateType.NEW
        }
    }

    def "Produce DROP updates for replaced block"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(GenericMultistream), Schedulers.boundedElastic())
        def block = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            parentHash = parent
            transactions = []
        })
        when:
        def act = connectBlockUpdates.whenReplaced(block, "ConnectBlockUpdatesSpec")
                .collectList().block(Duration.ofSeconds(3))

        then:
        act.size() == 1
        with(act[0]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.type == ConnectBlockUpdates.UpdateType.DROP
        }
    }

    def "Gets prev version if available"() {
        setup:
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(GenericMultistream), Schedulers.boundedElastic())
        def block1 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            parentHash = parent
            transactions = []
        })
        def block2 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            parentHash = parent
            transactions = []
        })
        def block3 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xdb1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7")
            number = 13412872
            totalDifficulty = BigInteger.ONE
            parentHash = parent
            timestamp = Instant.now()
            transactions = []
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
        def connectBlockUpdates = new ConnectBlockUpdates(Stub(GenericMultistream), Schedulers.boundedElastic())
        def block1 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            parentHash = parent
            transactions = []
        })
        def block2 = BlockContainer.from(new BlockJson<TransactionRefJson>().tap {
            hash = BlockHash.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            number = 13412871
            totalDifficulty = BigInteger.ONE
            timestamp = Instant.now()
            parentHash = parent
            transactions = []
        })

        when:
        connectBlockUpdates.remember(block1)
        def act = connectBlockUpdates.extract(block2)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 2
        with(act[0]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0xe5be2159b2b7daf6b126babdcbaa349da668b92d6b8c7db1350fd527fec4885c")
            it.type == ConnectBlockUpdates.UpdateType.DROP
        }
        with(act[1]) {
            it.blockNumber == 13412871
            it.blockHash == BlockId.from("0x668b92d6b8c7db1350fd527fec4885ce5be2159b2b7daf6b126babdcbaa349da")
            it.type == ConnectBlockUpdates.UpdateType.NEW
        }
    }

    def "Keeps connection"() {
        setup:
        def head = Mock(Head) {
            1 * getFlux() >> Flux.never()
        }
        def up = Mock(GenericMultistream) {
            1 * getHead(Selector.empty) >> head
        }
        def connectBlockUpdates = new ConnectBlockUpdates(up, Schedulers.boundedElastic())

        when:
        def a1 = connectBlockUpdates.connect()
        def a2 = connectBlockUpdates.connect()

        then:
        a1 == a2
    }
}
