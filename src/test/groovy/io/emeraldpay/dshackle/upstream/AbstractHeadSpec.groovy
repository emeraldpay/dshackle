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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class AbstractHeadSpec extends Specification {

    def blocks = [1L, 2, 3, 4].collect { i ->
        byte[] hash = new byte[32]
        hash[0] = i as byte
        new BlockContainer(i, BlockId.from(hash), BigInteger.valueOf(i), Instant.now(), false, null, null, [])
    }

    def "Calls beforeBlock on each block"() {
        setup:
        TopicProcessor<BlockContainer> source = TopicProcessor.create()
        def head = new TestHead()
        def called = false
        when:
        head.follow(Flux.from(source))
        head.onBeforeBlock {
            called = true
        }
        def act = head.flux
        source.onNext(blocks[0])
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .then {
                    assert called
                    called = false
                    source.onNext(blocks[1])
                }
                .expectNext(blocks[1])
                .then {
                    assert called
                    source.onComplete()
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Follows source"() {
        setup:
        TopicProcessor<BlockContainer> source = TopicProcessor.create()
        def head = new TestHead()
        when:
        head.follow(Flux.from(source))
        def act = head.flux
        source.onNext(blocks[0])
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .then { source.onNext(blocks[1]) }
                .expectNext(blocks[1])
                .then { source.onNext(blocks[2]) }
                .expectNext(blocks[2])
                .then { source.onNext(blocks[3]) }
                .expectNext(blocks[3])
                .then { source.onComplete() }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Ignores block will less difficulty"() {
        setup:
        TopicProcessor<BlockContainer> source = TopicProcessor.create()
        def head = new TestHead()
        def wrongblock = new BlockContainer(
                blocks[1].height, BlockId.from(blocks[1].hash.value.clone().tap { it[1] = 0xff as byte }),
                blocks[1].difficulty - 1,
                Instant.now(),
                false, null, null, []
        )
        when:
        head.follow(Flux.from(source))
        def act = head.flux
        source.onNext(blocks[0])
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .then { source.onNext(blocks[1]) }
                .expectNext(blocks[1])
                .then { source.onNext(wrongblock) }
                .then { source.onNext(blocks[3]) }
                .expectNext(blocks[3])
                .then { source.onComplete() }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    class TestHead extends AbstractHead {

    }
}
