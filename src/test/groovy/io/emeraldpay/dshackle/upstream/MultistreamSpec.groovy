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
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class MultistreamSpec extends Specification {

    def "Aggregates methods"() {
        setup:
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(), new DirectCallMethods(["eth_test1", "eth_test2"]))
        def up2 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(), new DirectCallMethods(["eth_test2", "eth_test3"]))
        def aggr = new EthereumMultistream(Chain.ETHEREUM, [up1, up2], Caches.default())
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

    def "Call postprocess after api use"() {
        setup:
        def request = new JsonRpcRequest("test_foo", [1], 1, null)

        def api = TestingCommons.api()
        api.answer("test_foo", [1], "test")
        def postprocessor = Mock(RequestPostprocessor)
        def up = TestingCommons.upstream(api)
        def multistream = new TestMultistream([up], postprocessor)

        when:
        def rdr = multistream.getDirectApi(Selector.empty).block(Duration.ofSeconds(1))
        def act = rdr.read(request).block(Duration.ofSeconds(1))

        then:
        act != null
        act.hasResult()
        act.resultAsProcessedString == "test"
        1 * postprocessor.onReceive("test_foo", [1], "\"test\"".bytes)
    }

    class TestMultistream extends Multistream {

        TestMultistream(List<Upstream> upstreams, @NotNull RequestPostprocessor postprocessor) {
            super(Chain.ETHEREUM, upstreams, Caches.default(), postprocessor)
        }

        @Override
        Reader<JsonRpcRequest, JsonRpcResponse> getLocalReader(@NotNull Selector.Matcher matcher) {
            return null
        }

        @Override
        Head updateHead() {
            return null
        }

        @Override
        void setHead(@NotNull Head head) {

        }

        @Override
        Head getHead() {
            return null
        }

        @Override
        Collection<UpstreamsConfig.Labels> getLabels() {
            return null
        }

        <T extends Upstream> T cast(Class<T> selfType) {
            return this
        }

        @Override
        ChainFees getFeeEstimation() {
            return null
        }

        @Override
        EgressSubscription getEgressSubscription() {
            return null
        }
    }
}
