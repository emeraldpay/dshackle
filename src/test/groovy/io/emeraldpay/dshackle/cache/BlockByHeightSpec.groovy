/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit

class BlockByHeightSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"

    ObjectMapper objectMapper = Global.objectMapper

    def "Fetch with all data available"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block = new BlockJson<TransactionRefJson>()
        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash1)
        block.number = 100
        block.hash = BlockHash.from(hash1)
        block.totalDifficulty = BigInteger.ONE
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.uncles = []
        block.transactions = List.of(tx)
        block.parentHash = BlockHash.from(hash1)

        BlockContainer.from(block).with {
            blocks.add(it)
            heights.add(it)
        }

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        objectMapper.readValue(act.json, BlockJson) == block
    }

    def "Fetch correct blocks if multiple"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block1 = new BlockJson<TransactionRefJson>()
        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash1)
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        block1.totalDifficulty = BigInteger.ONE
        block1.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block1.uncles = []
        block1.transactions = List.of(tx)
        block1.parentHash = BlockHash.from(hash1)

        def block2 = new BlockJson<TransactionRefJson>()
        block2.number = 101
        block2.hash = BlockHash.from(hash2)
        block2.totalDifficulty = BigInteger.ONE
        block2.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block2.uncles = []
        block2.transactions = List.of(tx)
        block2.parentHash = BlockHash.from(hash2)

        BlockContainer.from(block1).with {
            blocks.add(it)
            heights.add(it)
        }
        BlockContainer.from(block2).with {
            blocks.add(it)
            heights.add(it)
        }

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        objectMapper.readValue(act.json, BlockJson) == block1

        when:
        act = blocksByHeight.read(101).block()
        then:
        objectMapper.readValue(act.json, BlockJson) == block2
    }

    def "Fetch last block if updated"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block1 = new BlockJson<TransactionRefJson>()
        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash1)
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        block1.totalDifficulty = BigInteger.ONE
        block1.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block1.uncles = []
        block1.transactions = List.of(tx)
        block1.parentHash = BlockHash.from(hash1)

        def block2 = new BlockJson<TransactionRefJson>()
        block2.number = 100
        block2.hash = BlockHash.from(hash2)
        block2.totalDifficulty = BigInteger.ONE
        block2.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block2.uncles = []
        block2.transactions = List.of(tx)
        block2.parentHash = BlockHash.from(hash2)

        BlockContainer.from(block1).with {
            blocks.add(it)
            heights.add(it)
        }
        BlockContainer.from(block2).with {
            blocks.add(it)
            heights.add(it)
        }

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        objectMapper.readValue(act.json, BlockJson) == block2
    }

    def "Fetch nothing if block expired"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from(hash1)
        block.totalDifficulty = BigInteger.ONE
        block.timestamp = Instant.now()
        block.parentHash = BlockHash.from(hash1)

        // add only to heights
        BlockContainer.from(block).with {
            heights.add(it)
        }

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == null
    }

    def "Fetch nothing if height expired"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()

        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.hash = BlockHash.from(hash1)
        block.totalDifficulty = BigInteger.ONE
        block.timestamp = Instant.now()
        block.parentHash = BlockHash.from(hash1)

        // add only to blocks
        BlockContainer.from(block).with {
            blocks.add(it)
        }

        def blocksByHeight = new BlockByHeight(heights, blocks)

        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == null
    }

    def "Fetch nothing if both empty"() {
        setup:
        def blocks = new BlocksMemCache()
        def heights = new HeightCache()
        def blocksByHeight = new BlockByHeight(heights, blocks)
        when:
        def act = blocksByHeight.read(100).block()
        then:
        act == null
    }
}
