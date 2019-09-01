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
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ethereum.DirectEthereumApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWs
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.rpc.DefaultRpcClient
import spock.lang.Specification

class FilteringApiIteratorSpec extends Specification {

    def rpcClient = new DefaultRpcClient(null)
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
        def iter = new FilteringApiIterator(upstreams, 0, matcher, 1)
        then:
        iter.hasNext()
        iter.next() == upstreams[0].api
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        !iter.hasNext()

        when:
        iter = new FilteringApiIterator(upstreams, 1, matcher, 1)
        then:
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        !iter.hasNext()

        when:
        iter = new FilteringApiIterator(upstreams, 1,  matcher, 2)
        then:
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        iter.hasNext()
        iter.next() == upstreams[2].api
        iter.hasNext()
        iter.next() == upstreams[3].api
        iter.hasNext()
        iter.next() == upstreams[0].api
        !iter.hasNext()
    }
}
