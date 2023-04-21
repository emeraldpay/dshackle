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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.test.EthereumApiStub
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumRpcUpstream
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnectorFactory
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Retry
import spock.lang.Specification

import java.time.Duration

class FilteredApisSpec extends Specification {

    def ethereumTargets = new DefaultEthereumMethods(Chain.ETHEREUM)

    def "Verifies labels"() {
        setup:
        def i = 0
        List<EthereumRpcUpstream> upstreams = [
                [test: "foo"],
                [test: "bar"],
                [test: "foo", test2: "baz"],
                [test: "foo"],
                [test: "baz"]
        ].collect {
            def httpFactory = Mock(HttpFactory) {
                create(_, _) >> TestingCommons.api().tap { it.id = "${i++}" }
            }
            def connectorFactory = new EthereumConnectorFactory(
                    EthereumConnectorFactory.ConnectorMode.RPC_ONLY,
                    null,
                    httpFactory,
                    new MostWorkForkChoice(),
                    BlockValidator.ALWAYS_VALID,
                    Schedulers.parallel(),
                    Schedulers.parallel()
            )
            new EthereumRpcUpstream(
                    "test",
                    (byte) 123,
                    Chain.ETHEREUM,
                    new UpstreamsConfig.PartialOptions().buildOptions(),
                    UpstreamsConfig.UpstreamRole.PRIMARY,
                    ethereumTargets,
                    new QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(it)),
                    connectorFactory,
                    ChainsConfig.ChainConfig.default()
            )
        }
        def matcher = new Selector.LabelMatcher("test", ["foo"])
        upstreams.forEach {
            it.setLag(0)
            it.setStatus(UpstreamAvailability.OK)
        }
        when:
        def iter = new FilteredApis(Chain.ETHEREUM, upstreams, matcher, 0, 1, 0)
        iter.request(10)
        then:
        StepVerifier.create(iter)
                .expectNext(upstreams[0])
                .expectNext(upstreams[2])
                .expectNext(upstreams[3])
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        when:
        iter = new FilteredApis(Chain.ETHEREUM, upstreams, matcher, 1, 1, 0)
        iter.request(10)
        then:
        StepVerifier.create(iter)
                .expectNext(upstreams[2])
                .expectNext(upstreams[3])
                .expectNext(upstreams[0])
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        when:
        iter = new FilteredApis(Chain.ETHEREUM, upstreams, matcher, 1, 2, 0)
        iter.request(10)
        then:
        StepVerifier.create(iter)
                .expectNext(upstreams[2])
                .expectNext(upstreams[3])
                .expectNext(upstreams[0])
                .expectNext(upstreams[2])
                .expectNext(upstreams[3])
                .expectNext(upstreams[0])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Exponential backoff"() {
        setup:
        def apis = new FilteredApis(Chain.ETHEREUM, [], Selector.empty, 0, 1, 0)
        expect:
        wait == apis.waitDuration(n).toMillis() as Integer
        where:
        n  | wait
        0  | 100
        1  | 100
        2  | 400
        3  | 900
        4  | 1600
        5  | 2500
        6  | 3600
        7  | 4900
        8  | 5000
        9  | 5000
        10 | 5000
        -1 | 100
    }

    @Retry
    def "Backoff uses jitter"() {
        setup:
        def apis = new FilteredApis(Chain.ETHEREUM, [], Selector.empty, 0, 1, 20)
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
        def api1 = TestingCommons.api()
        def api2 = TestingCommons.api()
        def up1 = TestingCommons.upstream(api1)
        def up2 = TestingCommons.upstream(api2)
        then:
        StepVerifier.withVirtualTime({
            def apis = new FilteredApis(Chain.ETHEREUM, [up1, up2], Selector.empty, 0, 4, 0)
            apis.request(10)
            return apis
        })
                .expectNext(up1, up2).as("Batch 1")
                .expectNoEvent(Duration.ofMillis(100)).as("Wait 1")
                .expectNext(up1, up2).as("Batch 2")
                .expectNoEvent(Duration.ofMillis(400)).as("Wait 2")
                .expectNext(up1, up2).as("Batch 3")
                .expectNoEvent(Duration.ofMillis(900)).as("Wait 3")
                .expectNext(up1, up2).as("Batch 4")
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
        def act = new FilteredApis(Chain.ETHEREUM, ups, Selector.empty, 2, 1, 0)
        act.request(10)
        then:
        StepVerifier.create(act)
                .expectNext(ups[2], ups[3], ups[4], ups[5], ups[0], ups[1])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Start with offset - 5 items"() {
        expect:
        FilteredApis.startFrom([0, 1, 2, 3, 4], pos) == exp
        where:
        pos | exp
        0   | [0, 1, 2, 3, 4]
        1   | [1, 2, 3, 4, 0]
        2   | [2, 3, 4, 0, 1]
        3   | [3, 4, 0, 1, 2]
        4   | [4, 0, 1, 2, 3]
        5   | [0, 1, 2, 3, 4]
        6   | [1, 2, 3, 4, 0]
    }

    def "Start with offset - 2 items"() {
        expect:
        FilteredApis.startFrom([0, 1], pos) == exp
        where:
        pos | exp
        0   | [0, 1]
        1   | [1, 0]
        2   | [0, 1]
        3   | [1, 0]
        4   | [0, 1]
        5   | [1, 0]
        6   | [0, 1]
    }

    def "Starts with primary"() {
        setup:
        List<Upstream> standard = (0..1).collect {
            TestingCommons.upstream(
                    "test_" + it,
                    new EthereumApiStub(it)
            )
        }
        def fallback = [
                Mock(Upstream) {
                    _ * getRole() >> UpstreamsConfig.UpstreamRole.FALLBACK
                    _ * isAvailable() >> true
                    _ * getStatus() >> UpstreamAvailability.OK
                }
        ]
        when:
        def act = new FilteredApis(Chain.ETHEREUM,
                [] + fallback + standard,
                Selector.empty, 0, 3, 0)
        act.request(10)
        then:
        StepVerifier.create(act)
                .expectNext(standard[0], standard[1]).as("Initial requests")

                .expectNext(standard[0], standard[1]).as("Retry with standard")
                .expectNext(fallback[0]).as("Retry with fallback")

                .expectNext(standard[0], standard[1]).as("Second retry with standard")
                .expectNext(fallback[0]).as("Second retry with fallback")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Use secondary after primary"() {
        setup:
        List<Upstream> standard = (0..1).collect {
            TestingCommons.upstream(
                    "test_" + it,
                    new EthereumApiStub(it)
            )
        }
        List<Upstream> fallback = [
                Mock([name: "fallback"], Upstream) {
                    _ * getRole() >> UpstreamsConfig.UpstreamRole.FALLBACK
                    _ * isAvailable() >> true
                    _ * getStatus() >> UpstreamAvailability.OK
                }
        ]
        List<Upstream> secondary = [
                Mock([name: "secondary"], Upstream) {
                    _ * getRole() >> UpstreamsConfig.UpstreamRole.SECONDARY
                    _ * isAvailable() >> true
                    _ * getStatus() >> UpstreamAvailability.OK
                }
        ]
        when:
        def act = new FilteredApis(Chain.ETHEREUM,
                [] + fallback + standard + secondary,
                Selector.empty, 0, 3, 0)
        act.request(11)
        then:
        StepVerifier.create(act)
                .expectNext(standard[0], standard[1]).as("Initial requests with primary")
                .expectNext(secondary[0]).as("Initial requests with secondary")

                .expectNext(standard[0], standard[1]).as("Retry with primary")
                .expectNext(secondary[0]).as("Retry with secondary")
                .expectNext(fallback[0]).as("Retry with fallback")

                .expectNext(standard[0], standard[1]).as("Second retry with primary")
                .expectNext(secondary[0]).as("Second retry with secondary")
                .expectNext(fallback[0]).as("Second retry with fallback")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Use LAGGING after OK"() {
        setup:
        List<Upstream> lagging = [
                Mock([name: "lagging"], Upstream) {
                    _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
                    _ * isAvailable() >> true
                    _ * getStatus() >> UpstreamAvailability.LAGGING
                }
        ]
        List<Upstream> ok = [
                Mock([name: "ok"], Upstream) {
                    _ * getRole() >> UpstreamsConfig.UpstreamRole.PRIMARY
                    _ * isAvailable() >> true
                    _ * getStatus() >> UpstreamAvailability.OK
                }
        ]
        when:
        def act = new FilteredApis(Chain.ETHEREUM,
                [] + lagging + ok,
                Selector.empty, 0, 2, 0)
        act.request(4)
        then:
        StepVerifier.create(act)
                .expectNext(ok[0]).as("Initial requests with ok")
                .expectNext(lagging[0]).as("Initial requests with lagging")
                .expectNext(ok[0]).as("retry requests with ok")
                .expectNext(lagging[0]).as("retry requests with lagging")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
