/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.commons

import org.springframework.util.backoff.ExponentialBackOff
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Retry
import spock.lang.Specification

import java.time.Duration

class DurableFluxSpec extends Specification {

    private static CLOCK_ALLOW_ERROR_MS = 10

    def "Normal subscribe works"() {
        when:
        def flux = DurableFlux.newBuilder()
                .using({
                    Flux.fromIterable([1, 2, 3])
                })
                .build()
        def values = flux.connect().collectList().block(Duration.ofSeconds(1))

        then:
        values == [1, 2, 3]
    }

    def "Reconnects when broken"() {
        when:
        def connects = 0
        def flux = DurableFlux.newBuilder()
                .using({
                    connects++
                    def id = connects
                    Flux.fromIterable([100+id, 200+id, 300+id])
                            .concatWith(Mono.error(new RuntimeException("[TEST Reached the end of $id]")))
                })
                .backoffOnError(Duration.ofMillis(50))
                .build()
        def values = flux.connect()
                .take(5)
                .collectList().block(Duration.ofSeconds(1))

        then:
        values == [101, 201, 301, 102, 202]
    }

    @Retry // sometimes it goes too fast or too slow
    def "No continuous backoff if restored"() {
        when:
        def connects = 0
        def flux = DurableFlux.newBuilder()
                .using({
                    connects++
                    def id = connects
                    Flux.fromIterable([100+id, 200+id])
                            .concatWith(Mono.error(new RuntimeException("[TEST ERROR $id]")))
                })
                .backoffOnError(new ExponentialBackOff(100, 2))
                .build()
        def values = flux.connect()
                .take(7)

        def verifier = StepVerifier.create(values)
                .expectNext(101, 201)
                .expectNoEvent(Duration.ofMillis(100 - CLOCK_ALLOW_ERROR_MS))
                .expectNext(102, 202)
                .expectNoEvent(Duration.ofMillis(100 - CLOCK_ALLOW_ERROR_MS))
                .expectNext(103, 203)
                .expectNoEvent(Duration.ofMillis(100 - CLOCK_ALLOW_ERROR_MS))
                .expectNext(104)
                .expectComplete()
                .verifyLater()

        then:
        verifier.verify(Duration.ofSeconds(3))
    }

    @Retry // sometimes it goes too fast or too slow
    def "Continue backoff if not restored immediately"() {
        when:
        def connects = 0
        def flux = DurableFlux.newBuilder()
                .using({
                    connects++
                    def id = connects
                    if (id in [2, 4,5,6 ]) {
                        Flux.error(new RuntimeException("[TEST ERROR $id]"))
                    }  else {
                        Flux.fromIterable([100+id, 200+id])
                                .concatWith(Mono.error(new RuntimeException("[TEST ERROR $id]")))
                    }
                })
                .backoffOnError(new ExponentialBackOff(100, 2))
                .build()
        def values = flux.connect()
                .take(7)

        def verifier = StepVerifier.create(values)
                .expectNext(101, 201)
                .as("first batch")
                .expectNoEvent(Duration.ofMillis(100 + 200 - CLOCK_ALLOW_ERROR_MS))
                .as("immediate fail on #2")
                .expectNext(103, 203)
                .as("second batch")
                .expectNoEvent(Duration.ofMillis(100 + 200 + 400 + 800 - CLOCK_ALLOW_ERROR_MS))
                .as("three fails in row (after the original) as #4, #5, #6")
                .expectNext(107, 207)
                .as("third batch")
                .expectNoEvent(Duration.ofMillis(100 - CLOCK_ALLOW_ERROR_MS))
                .expectNext(108)
                .expectComplete()
                .verifyLater()

        then:
        verifier.verify(Duration.ofSeconds(3))

    }
}
