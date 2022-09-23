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
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.IntegrationTestingCommons
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import org.testcontainers.containers.GenericContainer
import org.testcontainers.spock.Testcontainers
import org.testcontainers.utility.DockerImageName
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit

@Testcontainers
class TxRedisCacheSpec extends Specification {

    GenericContainer redisContainer = new GenericContainer(
            DockerImageName.parse("redis:5.0.3-alpine")
    ).withExposedPorts(6379)

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash4 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"
    TxRedisCache cache

    ObjectMapper objectMapper = Global.objectMapper
    StatefulRedisConnection<String, byte[]> redis

    def setup() {
        redis = IntegrationTestingCommons.redisConnection(redisContainer.firstMappedPort)
        redis.sync().flushdb()
        cache = new TxRedisCache(redis.reactive(), Chain.ETHEREUM)
    }

    def cleanup() {
        redis.close()
    }

    def "Decode encoded"() {
        setup:
        TxContainer cont = new TxContainer(
                2000,
                TxId.from(hash1),
                BlockId.from(hash2),
                "test".bytes,
                null
        )
        when:
        def enc = cache.toProto(cont.hash, cont)
        def dec = cache.fromProto(enc)

        then:
        dec.height == 2000
        dec.hash.toHex() == hash1.substring(2)
        dec.blockId.toHex() == hash2.substring(2)
        dec.json == "test".bytes
        dec == cont
    }

    def "Add and read"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.totalDifficulty = BigInteger.ONE
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
        cache.add(TxContainer.from(tx), BlockContainer.from(block)).subscribe()
        def act = cache.read(TxId.from(hash1)).block()
        then:
        act != null
        objectMapper.readValue(act.json, TransactionJson) == tx
    }

    def "Evict single tx"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.totalDifficulty = BigInteger.ONE
        block.hash = BlockHash.from(hash2)
        block.transactions = []
        block.uncles = []

        def tx = new TransactionJson()
        tx.hash = TransactionId.from(hash3)
        tx.blockHash = block.hash
        tx.blockNumber = block.number
        tx.value = Wei.ofEthers(1.234)
        tx.nonce = 0

        when:
        cache.add(TxContainer.from(tx), BlockContainer.from(block)).subscribe()
        def act = cache.read(TxId.from(tx.hash)).block()
        then:
        act != null
        objectMapper.readValue(act.json, TransactionJson) == tx

        when:
        cache.evict(TxId.from(tx.hash)).subscribe()
        act = cache.read(TxId.from(tx.hash)).block()
        then:
        act == null
    }

    def "Evict all by block data"() {
        when:
        def block1 = new BlockJson<TransactionRefJson>()
        block1.hash = BlockHash.from(hash1)
        block1.number = 100
        block1.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block1.totalDifficulty = BigInteger.ONE
        block1.transactions = [
                new TransactionRefJson(TransactionId.from(hash1)),
                new TransactionRefJson(TransactionId.from(hash2)),
        ]
        def block2 = new BlockJson<TransactionRefJson>()
        block2.hash = BlockHash.from(hash2)
        block2.number = 101
        block2.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block2.totalDifficulty = BigInteger.ONE
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
            cache.add(TxContainer.from(tx), BlockContainer.from(block1)).subscribe()
        }
        [hash3, hash4].eachWithIndex{ String hash, int i ->
            def tx = new TransactionJson()
            tx.blockNumber = block2.number
            tx.blockHash = block2.hash
            tx.hash = TransactionId.from(hash)
            tx.value = Wei.ofEthers(i)
            tx.nonce = 0
            cache.add(TxContainer.from(tx), BlockContainer.from(block2)).subscribe()
        }


        cache.evict(BlockContainer.from(block1)).subscribe()

        def act1 = cache.read(TxId.from(hash1)).block()
        def act2 = cache.read(TxId.from(hash2)).block()
        def act3 = cache.read(TxId.from(hash3)).block()
        def act4 = cache.read(TxId.from(hash4)).block()

        then:
        act1 == null
        act2 == null
        act3 != null
        act3.hash.toHex() == hash3.substring(2)
        act4 != null
        act4.hash.toHex() == hash4.substring(2)
    }
}
