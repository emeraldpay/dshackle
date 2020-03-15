package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.test.IntegrationTestingCommons
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit

@IgnoreIf({ IntegrationTestingCommons.isDisabled("redis") })
class BlocksRedisCacheSpec extends Specification {

    StatefulRedisConnection<String, String> redis

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash4 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"


    def setup() {
        RedisClient client = IntegrationTestingCommons.redis()
        StatefulRedisConnection<String, String> connection = client.connect();
        connection.sync().flushdb()
        redis = connection
    }

    def "Add and read"() {
        setup:
        def cache = new BlocksRedisCache(
                redis.reactive(), Chain.ETHEREUM, TestingCommons.objectMapper()
        )
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.hash = BlockHash.from(hash1)
        block.transactions = []
        block.uncles = []

        when:
        cache.add(block).subscribe()
        def act = cache.read(BlockHash.from(hash1)).block()
        then:
        act == block
    }

    def "Evict existing block"() {
        setup:
        def cache = new BlocksRedisCache(
                redis.reactive(), Chain.ETHEREUM, TestingCommons.objectMapper()
        )
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.hash = BlockHash.from(hash2)
        block.transactions = []
        block.uncles = []

        when:
        cache.add(block).subscribe()
        def act = cache.read(BlockHash.from(hash2)).block()
        then:
        act == block

        when:
        cache.evict(block.hash).subscribe()
        act = cache.read(BlockHash.from(hash2)).block()

        then:
        act == null
    }

    def "Evict non-existing block"() {
        setup:
        def cache = new BlocksRedisCache(
                redis.reactive(), Chain.ETHEREUM, TestingCommons.objectMapper()
        )
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.hash = BlockHash.from(hash2)
        block.transactions = []
        block.uncles = []

        when:
        cache.add(block).subscribe()
        def act = cache.read(BlockHash.from(hash2)).block()
        then:
        act == block

        when:
        cache.evict(BlockHash.from(hash3)).subscribe()
        act = cache.read(BlockHash.from(hash2)).block()

        then:
        act == block
    }

}
