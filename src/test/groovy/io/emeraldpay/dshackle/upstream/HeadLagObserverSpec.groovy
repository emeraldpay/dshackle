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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.BlockJson
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import reactor.util.function.Tuples
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class HeadLagObserverSpec extends Specification {
    def parent = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")

    def "Updates lag distance"() {
        setup:
        Head master = Mock()

        Head head1 = Mock()
        Head head2 = Mock()

        Upstream up1 = Mock {
            _ * getHead() >> head1
        }
        Upstream up2 = Mock {
            _ * getHead() >> head2
        }

        def blocks = [100, 101, 102].collect { i ->
            return BlockContainer.from(
                    new BlockJson().tap {
                        it.number = i
                        it.parentHash = parent
                        it.totalDifficulty = 2000 + i
                        it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915" + i)
                        it.timestamp = Instant.now()
                    })
        }

        def masterBus = Sinks.many().multicast().onBackpressureBuffer()

        1 * master.getFlux() >> masterBus.asFlux()
        1 * head1.getFlux() >> Flux.merge(
                Flux.just(blocks[1]),
                Flux.just(blocks[2]).delaySubscription(Duration.ofSeconds(1))
                )
        1 * head2.getFlux() >> Flux.merge(
                Flux.just(blocks[0]),
                Flux.just(blocks[1]).delaySubscription(Duration.ofMillis(100))
        )
        1 * up1.setLag(0)
        1 * up2.setLag(1)
        1 * up2.setLag(0)

        HeadLagObserver observer = new TestHeadLagObserver(master, [up1, up2])
        when:
        def act = observer.subscription().take(Duration.ofMillis(5000))

        then:
        StepVerifier.create(act)
                .then { masterBus.tryEmitNext(blocks[1]) }
            .expectNextCount(3)
            .verifyComplete()
    }

    def "Probes until there is no difference"() {
        setup:
        Head master = Mock()
        HeadLagObserver observer = new TestHeadLagObserver(master, [])
        Upstream up = Mock()

        def blocks = [100, 101, 102].collect { i ->
            return BlockContainer.from(
                    new BlockJson().tap {
                        it.number = i
                        it.parentHash = parent
                        it.totalDifficulty = 2000 + i
                        it.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915" + i)
                        it.timestamp = Instant.now()
                    })
        }

        def upblocks = Flux.fromIterable(blocks)
        when:
        def act = observer.mapLagging(blocks[2], up, upblocks)
        then:
        StepVerifier.create(act)
                .expectNext(Tuples.of(2L, up))
                .expectNext(Tuples.of(1L, up))
                .expectNext(Tuples.of(0L, up))
                .verifyComplete()
    }

    class TestHeadLagObserver extends HeadLagObserver {

        TestHeadLagObserver(@NotNull Head master, @NotNull Collection<? extends Upstream> followers) {
            super(master, followers, DistanceExtractor.@Companion::extractPowDistance, Duration.ofNanos(1))
        }

        @Override
        long forkDistance(@NotNull BlockContainer top, @NotNull BlockContainer curr) {
            return 11
        }
    }
}
