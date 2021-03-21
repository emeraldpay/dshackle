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
import reactor.core.publisher.Mono
import spock.lang.Specification
import io.emeraldpay.dshackle.reader.Reader

import java.time.Instant

class HeightByHashAddingSpec extends Specification {

    def block = new BlockContainer(
            12079192L, BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"),
            BigInteger.ONE, Instant.now(), false, "".bytes, null, []
    )

    def "use memory if available"() {
        setup:
        def mem = new HeightByHashMemCache()
        def upstream = Mock(Reader)
        def reader = new HeightByHashAdding(
                mem, null, upstream
        )

        when:
        mem.add(block)
        def act = reader.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")).block()
        then:
        act == 12079192L
        0 * upstream.read(_)
    }

    def "call remote if not in memory and no redis"() {
        setup:
        def mem = new HeightByHashMemCache()
        def upstream = Mock(Reader)
        def reader = new HeightByHashAdding(
                mem, null, upstream
        )

        when:
        def act = reader.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")).block()
        then:
        act == 12079192L
        1 * upstream.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.just(block)
    }

    def "get from redis if not in memory"() {
        setup:
        def mem = new HeightByHashMemCache()
        def upstream = Mock(Reader)
        def redis = Mock(HeightByHashCache)
        def reader = new HeightByHashAdding(
                mem, redis, upstream
        )

        when:
        def act = reader.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")).block()
        then:
        act == 12079192L
        0 * upstream.read(_)
        1 * redis.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.just(12079192L)
    }

    def "call remote if not in memory and not in redis, add to redis"() {
        setup:
        def mem = new HeightByHashMemCache()
        def upstream = Mock(Reader)
        def redis = Mock(HeightByHashCache)
        def reader = new HeightByHashAdding(
                mem, redis, upstream
        )

        when:
        def act = reader.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")).block()
        then:
        act == 12079192L
        1 * redis.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.empty()
        1 * redis.add(block) >> Mono.just(true).then()
        1 * upstream.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.just(block)
    }

    def "empty is nowhere found, without redis"() {
        setup:
        def mem = new HeightByHashMemCache()
        def upstream = Mock(Reader)
        def reader = new HeightByHashAdding(
                mem, null, upstream
        )

        when:
        def act = reader.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")).block()
        then:
        act == null
        1 * upstream.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.empty()
    }

    def "empty is nowhere found, with redis"() {
        setup:
        def mem = new HeightByHashMemCache()
        def upstream = Mock(Reader)
        def redis = Mock(HeightByHashCache)
        def reader = new HeightByHashAdding(
                mem, redis, upstream
        )

        when:
        def act = reader.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")).block()
        then:
        act == null
        1 * redis.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.empty()
        1 * upstream.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.empty()
    }
}
