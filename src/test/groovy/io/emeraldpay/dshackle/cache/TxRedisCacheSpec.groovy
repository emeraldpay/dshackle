package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit

class TxRedisCacheSpec extends Specification {

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash4 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"
    TxRedisCache cache

    def setup() {
        RedisClient client = RedisClient.create("redis://localhost:6379");
        StatefulRedisConnection<String, String> connection = client.connect();
        connection.sync().flushdb()
        StatefulRedisConnection<String, String> redis = connection
        cache = new TxRedisCache(redis.reactive(), Chain.ETHEREUM, TestingCommons.objectMapper())
    }

    def "Add and read"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.hash = BlockHash.from(hash1)
        block.transactions = []
        block.uncles = []

        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash1)
        tx.blockHash = block.hash
        tx.blockNumber = block.number
        tx.value = Wei.ofEthers(1.234)
        tx.nonce = 0

        when:
        cache.add(tx, block).subscribe()
        def act = cache.read(TransactionId.from(hash1)).block()
        then:
        act == tx
    }

    def "Evict all by block data"() {
        when:
        def block1 = new BlockJson<TransactionRefJson>()
        block1.hash = BlockHash.from(hash1)
        block1.number = 100
        block1.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block1.transactions = [
                new TransactionRefJson(TransactionId.from(hash1)),
                new TransactionRefJson(TransactionId.from(hash2)),
        ]
        def block2 = new BlockJson<TransactionRefJson>()
        block2.hash = BlockHash.from(hash2)
        block2.number = 101
        block2.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block2.transactions = [
                new TransactionRefJson(TransactionId.from(hash3)),
                new TransactionRefJson(TransactionId.from(hash4)),
        ]

        [hash1, hash2].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = block1.number
            tx.blockHash = block1.hash
            tx.hash = TransactionId.from(hash)
            tx.value = Wei.ofEthers(i)
            tx.nonce = 0
            cache.add(tx, block1).subscribe()
        }
        [hash3, hash4].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = block2.number
            tx.blockHash = block2.hash
            tx.hash = TransactionId.from(hash)
            tx.value = Wei.ofEthers(i)
            tx.nonce = 0
            cache.add(tx, block2).subscribe()
        }


        cache.evict(block1).subscribe()

        def act1 = cache.read(TransactionId.from(hash1)).block()
        def act2 = cache.read(TransactionId.from(hash2)).block()
        def act3 = cache.read(TransactionId.from(hash3)).block()
        def act4 = cache.read(TransactionId.from(hash4)).block()

        then:
        act1 == null
        act2 == null
        act3 != null
        act3.hash.toHex() == hash3
        act4 != null
        act4.hash.toHex() == hash4
    }
}
