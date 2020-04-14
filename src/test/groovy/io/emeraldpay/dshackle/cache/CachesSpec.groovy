package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import spock.lang.Specification

import java.time.Instant

class CachesSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"

    ObjectMapper objectMapper = TestingCommons.objectMapper()

    def "Evict txes if block updated"() {
        setup:
        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock()
        def caches = Caches.newBuilder()
                .setObjectMapper(objectMapper)
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
        block1 = BlockContainer.from(block1, objectMapper)

        def block2 = new BlockJson()
        block2.number = 100
        block2.hash = BlockHash.from(hash2)
        block2.totalDifficulty = BigInteger.ONE
        block2.timestamp = Instant.now()
        block2.transactions = []
        block2 = BlockContainer.from(block2, objectMapper)

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
                .setObjectMapper(objectMapper)
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .build()

        def block1 = new BlockJson()
        block1.number = 100
        block1.hash = BlockHash.from(hash1)
        block1.totalDifficulty = BigInteger.ONE
        block1.timestamp = Instant.now()
        block1 = BlockContainer.from(block1, objectMapper)

        def block2 = new BlockJson()
        block2.number = 100
        block2.hash = BlockHash.from(hash2)
        block2.totalDifficulty = BigInteger.ONE
        block2.timestamp = Instant.now()
        block2 = BlockContainer.from(block2, objectMapper)

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
                .setObjectMapper(TestingCommons.objectMapper())
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
        caches.cache(Caches.Tag.REQUESTED, BlockContainer.from(block, objectMapper))
        then:
        0 * txCache.add(_)
    }

    def "Cache txes of a requested block"() {
        setup:
        TxMemCache txCache = Mock()
        HeightCache heightCache = Mock()
        BlocksMemCache blocksCache = Mock()
        def caches = Caches.newBuilder()
                .setObjectMapper(TestingCommons.objectMapper())
                .setTxByHash(txCache)
                .setBlockByHeight(heightCache)
                .setBlockByHash(blocksCache)
                .build()

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


        def block = new BlockJson()
        block.number = 100
        block.hash = BlockHash.from(hash1)
        block.totalDifficulty = BigInteger.ONE
        block.transactions = [tx1, tx2]
        block.timestamp = Instant.now()
        block = BlockContainer.from(block, objectMapper)

        when:
        caches.cache(Caches.Tag.REQUESTED, block)
        then:
        1 * txCache.add(TxContainer.from(tx1, objectMapper))
        1 * txCache.add(TxContainer.from(tx2, objectMapper))
    }
}
