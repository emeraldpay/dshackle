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
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Instant

class CachesSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"

    def "Evict txes if block updated"() {
        setup:
        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock()
        def caches = Caches.newBuilder()
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .build()

        def block1 = new BlockJson()
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        block1.totalDifficulty = BigInteger.ONE
        block1.timestamp = Instant.now()
        block1.transactions = []
        block1 = BlockContainer.from(block1)

        def block2 = new BlockJson()
        block2.number = 100
        block2.hash = BlockHash.from(hash2)
        block2.totalDifficulty = BigInteger.ONE
        block2.timestamp = Instant.now()
        block2.transactions = []
        block2 = BlockContainer.from(block2)

        when:
        caches.cache(Caches.Tag.LATEST, block1)
        then:
        1 * blocksCache.add(block1)
        1 * heightCache.add(block1) >> null

        when:
        caches.cache(Caches.Tag.LATEST, block2)
        then:
        1 * blocksCache.add(block2)
        1 * heightCache.add(block2) >> block1.hash
        1 * blocksCache.get(block1.hash) >> block1
        1 * txCache.evict(block1)
    }

    def "Evict txes if block updated - when block not cached"() {
        setup:
        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock()
        def caches = Caches.newBuilder()
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .build()

        def block1 = new BlockJson()
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        block1.totalDifficulty = BigInteger.ONE
        block1.timestamp = Instant.now()
        block1 = BlockContainer.from(block1)

        def block2 = new BlockJson()
        block2.number = 100
        block2.hash = BlockHash.from(hash2)
        block2.totalDifficulty = BigInteger.ONE
        block2.timestamp = Instant.now()
        block2 = BlockContainer.from(block2)

        when:
        caches.cache(Caches.Tag.LATEST, block1)
        then:
        1 * blocksCache.add(block1)
        1 * heightCache.add(block1) >> null

        when:
        caches.cache(Caches.Tag.LATEST, block2)
        then:
        1 * blocksCache.add(block2)
        1 * heightCache.add(block2) >> block1.hash
        1 * blocksCache.get(block1.hash) >> null
        1 * txCache.evict(block1.hash)
    }

    def "Do not cache txes of a requested block if it's just id"() {
        setup:
        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock()
        def caches = Caches.newBuilder()
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .build()

        def block = new BlockJson()
        block.number = 100
        block.hash = BlockHash.from(hash1)
        block.totalDifficulty = BigInteger.ONE
        block.timestamp = Instant.now()
        block.transactions = [
                new TransactionRefJson(TransactionId.from(hash1)),
                new TransactionRefJson(TransactionId.from(hash2)),
        ]

        when:
        caches.cache(Caches.Tag.REQUESTED, BlockContainer.from(block))
        then:
        0 * txCache.add(_)
    }

    def "Cache txes of a requested block"() {
        setup:

        def tx1 = new TransactionJson().with {
            hash = TransactionId.from(hash1)
            blockHash = BlockHash.from(hash1)
            blockNumber = 100
            it
        }
        def tx2 = new TransactionJson().with {
            hash = TransactionId.from(hash2)
            blockHash = BlockHash.from(hash1)
            blockNumber = 100
            it
        }
        BlockContainer block = new BlockJson().with { block ->
            block.number = 100
            block.hash = BlockHash.from(hash1)
            block.totalDifficulty = BigInteger.ONE
            block.transactions = [tx1, tx2]
            block.timestamp = Instant.now()
            BlockContainer.from(block)
        }


        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock() {
            _ * add(block)
            _ * read(block.hash) >> Mono.just(block)
        }
        TxRedisCache txRedisCache = Mock()
        def caches = Caches.newBuilder()
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .setTxByHash(txRedisCache)
                .build()

        when:
        caches.cache(Caches.Tag.REQUESTED, block)
        then:
        1 * txCache.add(TxContainer.from(tx1))
        1 * txCache.add(TxContainer.from(tx2))
        1 * txRedisCache.add(TxContainer.from(tx1), block) >> Mono.just(1).then()
        1 * txRedisCache.add(TxContainer.from(tx2), block) >> Mono.just(1).then()
    }

    def "Cache tx with redis"() {
        setup:

        def tx1 = new TransactionJson().with {
            hash = TransactionId.from(hash1)
            blockHash = BlockHash.from(hash1)
            blockNumber = 100
            it
        }
        BlockContainer block = new BlockJson().with { block ->
            block.number = 100
            block.hash = BlockHash.from(hash1)
            block.totalDifficulty = BigInteger.ONE
            block.transactions = [tx1]
            block.timestamp = Instant.now()
            BlockContainer.from(block)
        }
        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock()
        TxRedisCache txRedisCache = Mock()
        def caches = Caches.newBuilder()
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .setTxByHash(txRedisCache)
                .build()

        when:
        caches.cache(Caches.Tag.REQUESTED, TxContainer.from(tx1))
        then:
        1 * blocksCache.read(block.hash) >> Mono.just(block)
        1 * txRedisCache.add(TxContainer.from(tx1), block) >> Mono.just(1).then()
    }

    def "Put receipt into mem cache"() {
        setup:
        def receipt = new TransactionReceiptJson().tap {
            transactionHash = TransactionId.from("0xc7529e79f78f58125abafeaea01fe3abdc6f45c173d5dfb36716cbc526e5b2d1")
            blockHash = BlockHash.from("0x48249c81bfced2e6fe2536126471b73d83c4f21de75f88a16feb57cc566b991b")
            blockNumber = 0xccf6e2
            from = Address.from("0x3a1428354c99b119d891a30d326bad92e36e896a")
            logs = []
        }
        def receiptContainer = new DefaultContainer(
                TxId.from(receipt.transactionHash),
                BlockId.from(receipt.blockHash),
                receipt.blockNumber,
                Global.objectMapper.writeValueAsBytes(receipt),
                receipt
        )

        ReceiptMemCache receiptMemCache = Mock()
        def caches = Caches.newBuilder()
                .setReceipts(receiptMemCache)
                .build()
        Head head = Mock()
        caches.setHead(head)
        when:
        caches.cacheReceipt(Caches.Tag.REQUESTED, receiptContainer)

        then:
        1 * head.getCurrentHeight() >> 0xccf6e2
        1 * receiptMemCache.acceptsRecentBlocks(0) >> true
        1 * receiptMemCache.add(receiptContainer)
    }
}
