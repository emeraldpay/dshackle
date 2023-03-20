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

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.signature.NoSigner
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class MultistreamSpec extends Specification {

    def "Aggregates methods"() {
        setup:
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.standardApi(), new DirectCallMethods(["eth_test1", "eth_test2"]))
        def up2 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.standardApi(), new DirectCallMethods(["eth_test2", "eth_test3"]))
        def aggr = new EthereumMultistream(Chain.ETHEREUM, [up1, up2], Caches.default(), new NoSigner())
        when:
        aggr.onUpstreamsUpdated()
        def act = aggr.getMethods()
        then:
        act.isCallable("eth_test1")
        act.isCallable("eth_test2")
        act.isCallable("eth_test3")
        act.createQuorumFor("eth_test1") instanceof AlwaysQuorum
        act.createQuorumFor("eth_test2") instanceof AlwaysQuorum
        act.createQuorumFor("eth_test3") instanceof AlwaysQuorum
    }

    def "Filter Best Status accepts any input when none available "() {
        setup:
        def up = TestingCommons.upstream()
        def filter = new Multistream.FilterBestAvailability()
        def update1 = new Multistream.UpstreamStatus(
                up, UpstreamAvailability.LAGGING, Instant.now()
        )
        when:
        def act = filter.test(update1)
        then:
        act
    }

    def "Filter Best Status accepts better input"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def time0 = Instant.now() - Duration.ofSeconds(60)
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.LAGGING, time0
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.OK, time0
        )
        when:
        filter.test(update0)
        def act = filter.test(update1)
        then:
        act
    }

    def "Filter Best Status declines worse input"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def time0 = Instant.now() - Duration.ofSeconds(60)
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.LAGGING, time0
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.IMMATURE, time0
        )
        when:
        filter.test(update0)
        def act = filter.test(update1)
        then:
        !act
    }

    def "Filter Best Status accepts worse input from same upstream"() {
        setup:
        def up = TestingCommons.upstream("test-1")
        def time0 = Instant.now() - Duration.ofSeconds(60)
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up, UpstreamAvailability.LAGGING, time0
        )
        def update1 = new Multistream.UpstreamStatus(
                up, UpstreamAvailability.IMMATURE, time0
        )
        when:
        filter.test(update0)
        def act = filter.test(update1)
        then:
        act
    }

    def "Filter Best Status accepts any input if existing is outdated"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def time0 = Instant.now() - Duration.ofSeconds(90)
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.OK, time0
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.IMMATURE, time0 + Duration.ofSeconds(65)
        )
        when:
        filter.test(update0)
        def act = filter.test(update1)
        then:
        act
    }

    def "Filter Best Status declines same status"() {
        setup:
        def up1 = TestingCommons.upstream("test-1")
        def up2 = TestingCommons.upstream("test-2")
        def up3 = TestingCommons.upstream("test-3")
        def time0 = Instant.now() - Duration.ofSeconds(60)
        def filter = new Multistream.FilterBestAvailability()
        def update0 = new Multistream.UpstreamStatus(
                up1, UpstreamAvailability.OK, time0
        )
        def update1 = new Multistream.UpstreamStatus(
                up2, UpstreamAvailability.OK, time0
        )
        def update2 = new Multistream.UpstreamStatus(
                up3, UpstreamAvailability.OK, time0 + Duration.ofSeconds(10)
        )

        when:
        filter.test(update0)
        def act = filter.test(update1)
        then:
        !act

        when:
        act = filter.test(update2)
        then:
        !act
    }

}
