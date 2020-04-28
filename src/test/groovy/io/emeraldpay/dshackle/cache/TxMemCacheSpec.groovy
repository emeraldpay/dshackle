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
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import spock.lang.Specification

import java.time.Instant

class TxMemCacheSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash4 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"

    ObjectMapper objectMapper = TestingCommons.objectMapper()

    def "Add and read"() {
        setup:
        def cache = new TxMemCache()
        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash1)
        tx.blockHash = BlockHash.from(hash1)
        tx.blockNumber = 100

        when:
        cache.add(TxContainer.from(tx, objectMapper))
        def act = cache.read(TxId.from(hash1)).block()
        then:
        objectMapper.readValue(act.json, TransactionJson.class) == tx
    }

    def "Keeps only configured amount"() {
        setup:
        def cache = new TxMemCache(3)

        when:
        [hash1, hash2, hash3, hash4].eachWithIndex { String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100 + i
            tx.blockHash = BlockHash.from(hash)
            tx.hash = TransactionId.from(hash)
            cache.add(TxContainer.from(tx, objectMapper))
        }

        def act1 = cache.read(TxId.from(hash1)).block()
        def act2 = cache.read(TxId.from(hash2)).block()
        def act3 = cache.read(TxId.from(hash3)).block()
        def act4 = cache.read(TxId.from(hash4)).block()
        then:
        act2.hash.toHex() == hash2.substring(2)
        act3.hash.toHex() == hash3.substring(2)
        act4.hash.toHex() == hash4.substring(2)
        act1 == null
    }

    def "Evict all by block hash"() {
        setup:
        def cache = new TxMemCache()

        when:
        [hash1, hash2].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100
            tx.blockHash = BlockHash.from(hash1)
            tx.hash = TransactionId.from(hash)
            cache.add(TxContainer.from(tx, objectMapper))
        }
        [hash3, hash4].eachWithIndex { String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 101
            tx.blockHash = BlockHash.from(hash2)
            tx.hash = TransactionId.from(hash)
            cache.add(TxContainer.from(tx, objectMapper))
        }

        cache.evict(BlockId.from(hash1))

        def act1 = cache.read(TxId.from(hash1)).block()
        def act2 = cache.read(TxId.from(hash2)).block()
        def act3 = cache.read(TxId.from(hash3)).block()
        def act4 = cache.read(TxId.from(hash4)).block()

        then:
        act1 == null
        act2 == null
        act3.hash.toHex() == hash3.substring(2)
        act4.hash.toHex() == hash4.substring(2)
    }

    def "Evict all by block data"() {
        setup:
        def cache = new TxMemCache()

        when:
        [hash1, hash2].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100
            tx.blockHash = BlockHash.from(hash1)
            tx.hash = TransactionId.from(hash)
            cache.add(TxContainer.from(tx, objectMapper))
        }
        [hash3, hash4].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = 100
            tx.blockHash = BlockHash.from(hash2)
            tx.hash = TransactionId.from(hash)
            cache.add(TxContainer.from(tx, objectMapper))
        }

        def block = new BlockJson<TransactionRefJson>()
        block.hash = BlockHash.from(hash1)
        block.number = 100
        block.totalDifficulty = BigInteger.ONE
        block.timestamp = Instant.now()
        block.transactions = [
                new TransactionRefJson(TransactionId.from(hash1)),
                new TransactionRefJson(TransactionId.from(hash2)),
        ]

        cache.evict(BlockContainer.from(block, objectMapper))

        def act1 = cache.read(TxId.from(hash1)).block()
        def act2 = cache.read(TxId.from(hash2)).block()
        def act3 = cache.read(TxId.from(hash3)).block()
        def act4 = cache.read(TxId.from(hash4)).block()

        then:
        act1 == null
        act2 == null
        act3.hash.toHex() == hash3.substring(2)
        act4.hash.toHex() == hash4.substring(2)
    }
}
