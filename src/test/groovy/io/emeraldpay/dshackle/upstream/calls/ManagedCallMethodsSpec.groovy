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
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import spock.lang.Specification

class ManagedCallMethodsSpec extends Specification {

    def "Gets quorum for enabled method"() {
        setup:
        def managed = new ManagedCallMethods(
                new DirectCallMethods(["eth_test2", "foo_bar"] as Set),
                ["eth_test"] as Set,
                [] as Set
        )
        when:
        def act = managed.getQuorumFor("eth_test")
        then:
        act instanceof AlwaysQuorum
    }

    def "Allowed contacts all enabled + delegate"() {
        setup:
        def managed = new ManagedCallMethods(
                new DirectCallMethods(["eth_test2"] as Set),
                ["eth_test"] as Set,
                [] as Set
        )
        when:
        def act = managed.getSupportedMethods()
        then:
        act.sort() == ["eth_test", "eth_test2"].sort()
    }

    def "Disabled removed from delegate"() {
        setup:
        def managed = new ManagedCallMethods(
                new DirectCallMethods(["eth_test2", "foo_bar"] as Set),
                ["eth_test"] as Set,
                ["foo_bar"] as Set
        )
        when:
        def act = managed.getSupportedMethods()
        then:
        act.sort() == ["eth_test", "eth_test2"].sort()
    }

    def "Use quorum from delegate if it supports the method"() {
        setup:
        def delegated = ["eth_test", "eth_test2"] as Set
        def delegate = Mock(CallMethods) {
            _ * it.getSupportedMethods() >> delegated
            1 * it.getQuorumFor("eth_test") >> new BroadcastQuorum()
        }
        def managed = new ManagedCallMethods(
                delegate,
                ["eth_test"] as Set,
                ["foo_bar"] as Set
        )
        when:
        def act = managed.getQuorumFor("eth_test")
        then:
        act != null
        act instanceof BroadcastQuorum
    }
}
