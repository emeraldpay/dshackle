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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import spock.lang.Specification

class AggregatedCallMethodsSpec extends Specification {

    def "Returns quorum from delegate that owns it"() {
        setup:
        def quorum = new AlwaysQuorum()
        def delegate1 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_no_test", "foo_bar"]
            1 * isCallable("eth_test") >> false
        }
        def delegate2 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_test", "foo_bar"]
            1 * isCallable("eth_test") >> true
            1 * createQuorumFor("eth_test") >> quorum
        }
        def aggregate = new AggregatedCallMethods([delegate1, delegate2])
        when:
        def act = aggregate.createQuorumFor("eth_test")
        then:
        act == quorum
    }

    def "Allowed if any allowed"() {
        setup:
        def delegate1 = new DirectCallMethods(["eth_no_test", "foo_bar"] as Set)
        def delegate2 = new DirectCallMethods(["eth_test", "foo_bar"] as Set)
        def aggregate = new AggregatedCallMethods([delegate1, delegate2])
        when:
        def act = aggregate.isCallable("eth_test")
        then:
        act

        when:
        act = aggregate.isCallable("eth_no_test")
        then:
        act

        when:
        act = aggregate.isCallable("foo_bar")
        then:
        act

        when:
        act = aggregate.isCallable("nothing")
        then:
        !act
    }

    def "Supported has all methods"() {
        setup:
        def delegate1 = new DirectCallMethods(["eth_no_test", "foo_bar"] as Set)
        def delegate2 = new DirectCallMethods(["eth_test", "foo_bar"] as Set)
        def aggregate = new AggregatedCallMethods([delegate1, delegate2])
        when:
        def act = aggregate.getSupportedMethods()
        then:
        act.sort() == ["eth_test", "eth_no_test", "foo_bar"].sort()
    }

    def "Hardcoded if any hardcoded"() {
        setup:
        def delegate1 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_no_test", "foo_bar"]
            1 * isHardcoded("eth_no_test") >> false
        }
        def delegate2 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_test", "foo_bar"]
            1 * isHardcoded("eth_test") >> true
        }
        def aggregate = new AggregatedCallMethods([delegate1, delegate2])
        when:
        def act = aggregate.isHardcoded("eth_test")
        then:
        act

        when:
        act = aggregate.isHardcoded("eth_no_test")
        then:
        !act
    }

    def "Can be hardcoded if not allowed"() {
        setup:
        def delegate1 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_no_test", "foo_bar"]
            _ * isCallable(_) >> false
            1 * isHardcoded("eth_no_test") >> false
        }
        def delegate2 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_test", "foo_bar"]
            _ * isCallable(_) >> false
            1 * isHardcoded("eth_test") >> true
        }
        def aggregate = new AggregatedCallMethods([delegate1, delegate2])
        when:
        def act = aggregate.isHardcoded("eth_test")
        then:
        act

        when:
        act = aggregate.isHardcoded("eth_no_test")
        then:
        !act
    }

    def "Execute hardcoded on delegate that owns it"() {
        setup:
        def delegate1 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_no_test", "foo_bar"]
            1 * isHardcoded("eth_test") >> false
        }
        def delegate2 = Mock(CallMethods) {
            _ * getSupportedMethods() >> ["eth_test", "foo_bar"]
            1 * isHardcoded("eth_test") >> true
            1 * executeHardcoded("eth_test") >> "hello"
        }
        def aggregate = new AggregatedCallMethods([delegate1, delegate2])
        when:
        def act = aggregate.executeHardcoded("eth_test")
        then:
        new String(act) == "hello"
    }
}
