/**
 * Copyright (c) 2019 ETCDEV GmbH
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
import io.emeraldpay.dshackle.test.EthereumApiStub
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWs
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.ReactorRpcClient
import reactor.test.StepVerifier
import spock.lang.Retry
import spock.lang.Specification

import java.time.Duration

class FilteredApisSpec extends Specification {

    def rpcClient = Stub(ReactorRpcClient)
    def objectMapper = TestingCommons.objectMapper()
    def ethereumTargets = new QuorumBasedMethods(objectMapper, Chain.ETHEREUM)

    def "Verifies labels"() {
        setup:
        List<EthereumUpstream> upstreams = [
                [test: "foo"],
                [test: "bar"],
                [test: "foo", test2: "baz"],
                [test: "foo"],
                [test: "baz"]
        ].collect {
            new EthereumUpstream(
                    "test",
                    Chain.ETHEREUM,
                    new DirectEthereumApi(rpcClient, objectMapper, ethereumTargets),
                    (EthereumWs) null,
                    new UpstreamsConfig.Options(),
                    new NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels.fromMap(it)),
                    ethereumTargets
            )
        }
        def matcher = new Selector.LabelMatcher("test", ["foo"])
        upstreams.forEach {
            it.setLag(0)
            it.setStatus(UpstreamAvailability.OK)
        }
        when:
        def iter = new FilteredApis(upstreams, matcher, 0, 1, 0)
        iter.request(10)
        then:
        StepVerifier.create(iter)
            .expectNext(upstreams[0].api)
            .expectNext(upstreams[2].api)
            .expectNext(upstreams[3].api)
            .expectComplete()
            .verify(Duration.ofSeconds(1))

        when:
        iter = new FilteredApis(upstreams, matcher, 1, 1, 0)
        iter.request(10)
        then:
        StepVerifier.create(iter)
                .expectNext(upstreams[2].api)
                .expectNext(upstreams[3].api)
                .expectNext(upstreams[0].api)
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        when:
        iter = new FilteredApis(upstreams, matcher, 1, 2, 0)
        iter.request(10)
        then:
        StepVerifier.create(iter)
                .expectNext(upstreams[2].api)
                .expectNext(upstreams[3].api)
                .expectNext(upstreams[0].api)
                .expectNext(upstreams[2].api)
                .expectNext(upstreams[3].api)
                .expectNext(upstreams[0].api)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Exponential backoff"() {
        setup:
        def apis = new FilteredApis([], Selector.empty, 0, 1, 0)
        expect:
        wait == apis.waitDuration(n).toMillis() as Integer
        where:
        n   | wait
        0   | 100
        1   | 100
        2   | 400
        3   | 900
        4   | 1600
        5   | 2500
        6   | 3600
        7   | 4900
        8   | 5000
        9   | 5000
        10  | 5000
        -1  | 100
    }

    @Retry
    def "Backoff uses jitter"() {
        setup:
        def apis = new FilteredApis([], Selector.empty, 0, 1, 20)
        when:
        def act = apis.waitDuration(1).toMillis()
        println act
        then:
        act >= 80
        act <= 120
        act != 100

        when:
        act = apis.waitDuration(3).toMillis()
        println act
        then:
        act >= 900 - 9 * 20
        act <= 900 + 9 * 20
        act != 900
    }

    def "Makes pause between batches"() {
        when:
        def api1 = TestingCommons.api(Stub(ReactorRpcClient))
        def api2 = TestingCommons.api(Stub(ReactorRpcClient))
        def up1 = TestingCommons.upstream(api1)
        def up2 = TestingCommons.upstream(api2)
        then:
        StepVerifier.withVirtualTime({
            def apis = new FilteredApis([up1, up2], Selector.empty, 0, 4, 0)
            apis.request(10)
            return apis
        })
        .expectNext(api1, api2).as("Batch 1")
        .expectNoEvent(Duration.ofMillis(100)).as("Wait 1")
        .expectNext(api1, api2).as("Batch 2")
        .expectNoEvent(Duration.ofMillis(400)).as("Wait 2")
        .expectNext(api1, api2).as("Batch 3")
        .expectNoEvent(Duration.ofMillis(900)).as("Wait 3")
        .expectNext(api1, api2).as("Batch 4")
        .expectComplete()
        .verify(Duration.ofSeconds(10))
    }

    def "Starts with right position"() {
        setup:
        def apis = (0..5).collect {
            new EthereumApiStub(it)
        }
        def ups = apis.collect {
            TestingCommons.upstream(it)
        }
        when:
        def act = new FilteredApis(ups, Selector.empty, 2, 1, 0)
        act.request(10)
        then:
        StepVerifier.create(act)
            .expectNext(apis[2], apis[3], apis[4], apis[5], apis[0], apis[1])
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }
}
