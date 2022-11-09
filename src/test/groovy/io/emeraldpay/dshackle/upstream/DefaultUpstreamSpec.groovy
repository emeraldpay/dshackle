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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.ForkWatchMock
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class DefaultUpstreamSpec extends Specification {

    def "Availability by status produce current status"() {
        setup:
        def upstream = new DefaultUpstreamTestImpl("test", new ForkWatch.Never())
        upstream.setStatus(UpstreamAvailability.LAGGING)
        when:
        def act = upstream.getAvailabilityByStatus().take(1)
                .collectList().block()
        then:
        act == [UpstreamAvailability.LAGGING]
    }

    def "Availability by status produce updated status"() {
        setup:
        def upstream = new DefaultUpstreamTestImpl("test", new ForkWatch.Never())
        upstream.setStatus(UpstreamAvailability.LAGGING)
        when:
        def act = upstream.getAvailabilityByStatus().take(2)
        then:
        StepVerifier.create(act)
                .expectNext(UpstreamAvailability.LAGGING)
                .then {
                    upstream.setStatus(UpstreamAvailability.OK)
                }
                // syncing because we didn't not set the height lag
                .expectNext(UpstreamAvailability.SYNCING)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Availability for always valid upstream doesn't react on old height"() {
        setup:
        def upstream = new DefaultUpstreamTestImpl("test",
                new ForkWatch.Never(),
                UpstreamsConfig.PartialOptions.getDefaults().tap {
                    it.disableValidation = true
                }.build()
        )
        when: "we have a zero height"
        upstream.setStatus(UpstreamAvailability.OK)
        def act = upstream.getStatus()
        then:
        act == UpstreamAvailability.OK

        when: "we set large lag"
        upstream.setLag(100)
        upstream.setStatus(UpstreamAvailability.OK)
        act = upstream.getStatus()
        then:
        act == UpstreamAvailability.OK
    }

    def "Can turn off always valid upstream without making it as lagging"() {
        setup:
        def upstream = new DefaultUpstreamTestImpl("test",
                new ForkWatch.Never(),
                UpstreamsConfig.PartialOptions.getDefaults().tap {
                    it.disableValidation = true
                }.build()
        )
        upstream.setStatus(UpstreamAvailability.UNAVAILABLE)
        when:
        def act = upstream.getAvailabilityByStatus().take(3)
        then:
        StepVerifier.create(act)
                .expectNext(UpstreamAvailability.UNAVAILABLE)
                .then { upstream.setStatus(UpstreamAvailability.OK) }
                // syncing because we didn't not set the height lag
                .expectNext(UpstreamAvailability.OK)
                .then { upstream.setStatus(UpstreamAvailability.OK) }
                .expectNoEvent(Duration.ofMillis(50))
                .then { upstream.setStatus(UpstreamAvailability.UNAVAILABLE) }
                // syncing because we didn't not set the height lag
                .expectNext(UpstreamAvailability.UNAVAILABLE)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Availability by fork produce current status"() {
        setup:
        def results = Sinks.many().unicast().<Boolean>onBackpressureBuffer()
        def upstream = new DefaultUpstreamTestImpl("test", new ForkWatchMock(results.asFlux()))
        when:
        def act = upstream.getAvailabilityByForks().take(1)
        then:
        StepVerifier.create(act)
                .expectNext(UpstreamAvailability.OK)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Availability by fork produce changed status"() {
        setup:
        def results = Sinks.many().unicast().<Boolean>onBackpressureBuffer()
        def upstream = new DefaultUpstreamTestImpl("test", new ForkWatchMock(results.asFlux()))
        when:
        def act = upstream.getAvailabilityByForks().take(2)
        then:
        StepVerifier.create(act)
                .expectNext(UpstreamAvailability.OK)
                .then {
                    results.tryEmitNext(true)
                }
                .expectNext(UpstreamAvailability.IMMATURE)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    class DefaultUpstreamTestImpl extends DefaultUpstream {

        DefaultUpstreamTestImpl(@NotNull String id,
                                @NotNull ForkWatch forkWatch) {
            this(id, forkWatch, UpstreamsConfig.PartialOptions.getDefaults().build())
        }

        DefaultUpstreamTestImpl(@NotNull String id,
                                @NotNull ForkWatch forkWatch,
                                @NotNull UpstreamsConfig.Options options) {
            super(id, Chain.ETHEREUM, forkWatch, options, UpstreamsConfig.UpstreamRole.PRIMARY, new DefaultEthereumMethods(Chain.ETHEREUM))
        }

        @Override
        Head getHead() {
            return null
        }

        @Override
        Reader<JsonRpcRequest, JsonRpcResponse> getIngressReader() {
            return null
        }

        @Override
        Collection<UpstreamsConfig.Labels> getLabels() {
            return null
        }

        @Override
        Set<Capability> getCapabilities() {
            return null
        }

        @Override
        boolean isGrpc() {
            return false
        }

        @Override
        def <T extends Upstream> T cast(@NotNull Class<T> selfType) {
            return null
        }
    }
}
