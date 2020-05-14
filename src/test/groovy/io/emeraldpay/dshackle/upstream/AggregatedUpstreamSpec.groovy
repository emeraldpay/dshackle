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
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainUpstream
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class AggregatedUpstreamSpec extends Specification {

    def "Aggregates methods"() {
        setup:
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(), new DirectCallMethods(["eth_test1", "eth_test2"]))
        def up2 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.api(), new DirectCallMethods(["eth_test2", "eth_test3"]))
        def aggr = new EthereumChainUpstream(Chain.ETHEREUM, [up1, up2], Caches.default(TestingCommons.objectMapper()), TestingCommons.objectMapper())
        when:
        aggr.onUpstreamsUpdated()
        def act = aggr.getMethods()
        then:
        act.isAllowed("eth_test1")
        act.isAllowed("eth_test2")
        act.isAllowed("eth_test3")
        act.getQuorumFor("eth_test1") instanceof AlwaysQuorum
        act.getQuorumFor("eth_test2") instanceof AlwaysQuorum
        act.getQuorumFor("eth_test3") instanceof AlwaysQuorum
    }
}
