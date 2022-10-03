/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Instant

class DefaultEthereumHeadSpec extends Specification {

    DefaultEthereumHead head = new DefaultEthereumHead("upstream", new MostWorkForkChoice(), BlockValidator.@Companion.ALWAYS_VALID)
    ObjectMapper objectMapper = Global.objectMapper

    def blocks = (10L..20L).collect { i ->
        BlockContainer.from(
                new BlockJson().tap {
                    it.number = 10000L + i
                    it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec89152" + i)
                    it.totalDifficulty = 11 * i
                    it.timestamp = Instant.now()
                })
    }

    def "Starts to follow"() {
        when:
        head.follow(Flux.just(blocks[0]))
        def act = head.flux
        then:
        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectComplete()
    }

    def "Follows normal order"() {
        when:
        head.follow(Flux.just(blocks[0], blocks[1], blocks[3]))
        def act = head.flux
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .expectNext(blocks[1])
                .expectNext(blocks[3])
                .expectComplete()
    }

    def "Ignores old"() {
        when:
        head.follow(Flux.just(blocks[0], blocks[3], blocks[1]))
        def act = head.flux
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .expectNext(blocks[3])
                .expectComplete()
    }

    def "Ignores repeating"() {
        when:
        head.follow(Flux.just(blocks[0], blocks[3], blocks[3], blocks[3], blocks[2], blocks[3]))
        def act = head.flux
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .expectNext(blocks[3])
                .expectComplete()
    }

    def "Ignores less difficult"() {
        when:
        def block3less = BlockContainer.from(
                new BlockJson().tap {
                    it.number = blocks[3].height
                    it.hash = BlockHash.from(blocks[3].hash.value)
                    it.totalDifficulty = blocks[3].difficulty - 1
                    it.timestamp = Instant.now()
                })
        head.follow(Flux.just(blocks[0], blocks[3], block3less))
        def act = head.flux
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .expectNext(blocks[3])
                .expectComplete()
    }

    def "Replaces with more difficult"() {
        when:
        def block3less = BlockContainer.from(
                new BlockJson().tap {
                    it.number = blocks[3].height
                    it.hash = BlockHash.from(blocks[3].hash.value)
                    it.totalDifficulty = blocks[3].difficulty + 1
                    it.timestamp = Instant.now()
                })
        head.follow(Flux.just(blocks[0], blocks[3], block3less))
        def act = head.flux
        then:
        StepVerifier.create(act)
                .expectNext(blocks[0])
                .expectNext(block3less)
                .expectComplete()
    }
}
