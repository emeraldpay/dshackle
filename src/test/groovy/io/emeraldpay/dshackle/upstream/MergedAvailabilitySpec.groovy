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
package io.emeraldpay.dshackle.upstream

import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class MergedAvailabilitySpec extends Specification {

    def "Produces the worst status of two"() {
        expect:
        def act = new MergedAvailability(
                Mono.just(left), Mono.just(right)
        ).produce().collectList().block()
        act == [exp]

        where:
        left                            | right                         | exp
        UpstreamAvailability.OK         | UpstreamAvailability.IMMATURE | UpstreamAvailability.IMMATURE
        UpstreamAvailability.LAGGING    | UpstreamAvailability.IMMATURE | UpstreamAvailability.IMMATURE
        UpstreamAvailability.IMMATURE   | UpstreamAvailability.IMMATURE | UpstreamAvailability.IMMATURE
        UpstreamAvailability.SYNCING    | UpstreamAvailability.IMMATURE | UpstreamAvailability.SYNCING
        UpstreamAvailability.UNAVAILABLE| UpstreamAvailability.IMMATURE | UpstreamAvailability.UNAVAILABLE
        UpstreamAvailability.OK         | UpstreamAvailability.SYNCING  | UpstreamAvailability.SYNCING
        UpstreamAvailability.UNAVAILABLE| UpstreamAvailability.SYNCING  | UpstreamAvailability.UNAVAILABLE
        UpstreamAvailability.LAGGING    | UpstreamAvailability.OK       | UpstreamAvailability.LAGGING
        UpstreamAvailability.SYNCING    | UpstreamAvailability.OK       | UpstreamAvailability.SYNCING
    }

    def "Changes status when the worst improves"() {
        setup:
        def left = Sinks.many().unicast().<UpstreamAvailability>onBackpressureBuffer()
        def right = Sinks.many().unicast().<UpstreamAvailability>onBackpressureBuffer()
        when:
        def act = new MergedAvailability(
                left.asFlux(),
                right.asFlux()
        ).produce()
        then:
        StepVerifier.create(act)
            .then {
                left.tryEmitNext(UpstreamAvailability.OK)
                right.tryEmitNext(UpstreamAvailability.IMMATURE)
            }
            .expectNext(UpstreamAvailability.IMMATURE)
            .then {
                right.tryEmitNext(UpstreamAvailability.OK)
            }
            .expectNext(UpstreamAvailability.OK)
            .verifyTimeout(Duration.ofSeconds(1))
    }

    def "Changes status when gets worse"() {
        setup:
        def left = Sinks.many().unicast().<UpstreamAvailability>onBackpressureBuffer()
        def right = Sinks.many().unicast().<UpstreamAvailability>onBackpressureBuffer()
        when:
        def act = new MergedAvailability(
                left.asFlux(),
                right.asFlux()
        ).produce()
        then:
        StepVerifier.create(act)
                .then {
                    left.tryEmitNext(UpstreamAvailability.OK)
                    right.tryEmitNext(UpstreamAvailability.OK)
                }
                .expectNext(UpstreamAvailability.OK)
                .then {
                    right.tryEmitNext(UpstreamAvailability.LAGGING)
                }
                .expectNext(UpstreamAvailability.LAGGING)
                .then {
                    left.tryEmitNext(UpstreamAvailability.UNAVAILABLE)
                }
                .expectNext(UpstreamAvailability.UNAVAILABLE)
                .verifyTimeout(Duration.ofSeconds(1))
    }

}
