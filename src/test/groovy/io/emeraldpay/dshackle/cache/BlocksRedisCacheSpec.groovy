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
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.IntegrationTestingCommons
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.lettuce.core.api.StatefulRedisConnection
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit

@IgnoreIf({ IntegrationTestingCommons.isDisabled("redis") })
class BlocksRedisCacheSpec extends Specification {

    StatefulRedisConnection<String, byte[]> redis
    BlocksRedisCache cache

    String hash1 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"

    ObjectMapper objectMapper = Global.objectMapper

    def setup() {
        redis = IntegrationTestingCommons.redisConnection()
        redis.sync().flushdb()
        cache = new BlocksRedisCache(
                redis.reactive(), Chain.ETHEREUM
        )
    }

    def "Decode encoded"() {
        setup:
        BlockContainer cont = new BlockContainer(
                100,
                BlockId.from(hash3),
                BigInteger.valueOf(10515),
                Instant.ofEpochSecond(10501050),
                false,
                "test".bytes,
                null,
                [TxId.from(hash2), TxId.from(hash1)], 0, "BlocksRedisCacheSpec"
        )

        when:
        def enc = cache.toProto(cont, cont)
        def dec = cache.deserializeValue(enc)

        then:
        dec.height == 100
        dec.hash.toHex() == hash3.substring(2)
        dec.difficulty.toString() == "10515"
        dec.timestamp == Instant.ofEpochSecond(10501050)
        dec.json == "test".bytes
        dec.transactions.size() == 2
        dec.transactions[0].toHex() == hash2.substring(2)
        dec.transactions[1].toHex() == hash1.substring(2)
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

        when:
        cache.add(BlockContainer.from(block)).subscribe()
        def act = cache.read(BlockId.from(hash1)).block()
        then:
        act != null
        objectMapper.readValue(act.json, BlockJson) == block
    }

    def "Evict existing block"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.totalDifficulty = BigInteger.ONE
        block.hash = BlockHash.from(hash2)
        block.transactions = []
        block.uncles = []

        when:
        cache.add(BlockContainer.from(block)).subscribe()
        def act = cache.read(BlockId.from(hash2)).block()
        then:
        objectMapper.readValue(act.json, BlockJson) == block

        when:
        cache.evict(BlockId.from(block.hash)).subscribe()
        act = cache.read(BlockId.from(hash2)).block()

        then:
        act == null
    }

    def "Evict non-existing block"() {
        setup:
        def block = new BlockJson<TransactionRefJson>()
        block.number = 100
        block.timestamp = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.SECONDS)
        block.totalDifficulty = BigInteger.ONE
        block.hash = BlockHash.from(hash2)
        block.transactions = []
        block.uncles = []

        when:
        cache.add(BlockContainer.from(block)).subscribe()
        def act = cache.read(BlockId.from(hash2)).block()
        then:
        act != null
        objectMapper.readValue(act.json, BlockJson) == block

        when:
        cache.evict(BlockId.from(hash3)).subscribe()
        act = cache.read(BlockId.from(hash2)).block()

        then:
        act != null
        objectMapper.readValue(act.json, BlockJson) == block
    }

}
