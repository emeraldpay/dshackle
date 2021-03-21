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
package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.test.IntegrationTestingCommons
import io.emeraldpay.grpc.Chain
import io.lettuce.core.api.StatefulRedisConnection
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.time.Instant

@IgnoreIf({ IntegrationTestingCommons.isDisabled("redis") })
class HeightByHashRedisCacheSpec extends Specification {

    def block1 = new BlockContainer(
            12079192L, BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"),
            BigInteger.ONE, Instant.now(), false, "".bytes, null, []
    )
    def block2 = new BlockContainer(
            12079193L, BlockId.from("0xd27944b460632699768fbfec3e5d454db590cae43d470b5f42fc4d091e372c25"),
            BigInteger.ONE, Instant.now(), false, "".bytes, null, []
    )

    StatefulRedisConnection<String, byte[]> redis
    HeightByHashRedisCache cache

    def setup() {
        redis = IntegrationTestingCommons.redisConnection()
        redis.sync().flushdb()
        cache = new HeightByHashRedisCache(
                redis.reactive(), Chain.ETHEREUM
        )
    }

    def "Add and read"() {
        when:
        cache.add(block1).subscribe()
        def act = cache.read(block1.hash).block()
        then:
        act == 12079192L
    }

    def "Add and read multiple"() {
        when:
        cache.add(block1).subscribe()
        cache.add(block2).subscribe()

        def act = cache.read(block1.hash).block()
        then:
        act == 12079192L

        def act2 = cache.read(block2.hash).block()
        then:
        act2 == 12079193L
    }
}
