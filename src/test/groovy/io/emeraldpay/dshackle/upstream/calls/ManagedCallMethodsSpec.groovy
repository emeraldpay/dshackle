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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.quorum.*
import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ManagedCallMethodsSpec extends Specification {

    def "Gets quorum for enabled method"() {
        setup:
        def managed = new ManagedCallMethods(
                new DirectCallMethods(["eth_test2", "foo_bar"] as Set),
                ["eth_test"] as Set,
                [] as Set,
                [] as Set,
                [] as Set
        )
        when:
        def act = managed.createQuorumFor("eth_test")
        then:
        act instanceof AlwaysQuorum
    }

    def "Allowed contacts all enabled + delegate"() {
        setup:
        def managed = new ManagedCallMethods(
                new DirectCallMethods(["eth_test2"] as Set),
                ["eth_test"] as Set,
                [] as Set,
                [] as Set,
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
                ["foo_bar"] as Set,
                [] as Set,
                [] as Set
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
            0 * it.createQuorumFor("eth_test") >> new BroadcastQuorum()
        }
        def managed = new ManagedCallMethods(
                delegate,
                ["eth_test"] as Set,
                ["foo_bar"] as Set,
                [] as Set,
                [] as Set
        )
        when:
        def act = managed.createQuorumFor("eth_test")
        then:
        act != null
        act instanceof AlwaysQuorum
    }

    def "Use custom quorum if provided"() {
        setup:
        def managed = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM),
                ["eth_test", "eth_foo", "eth_bar"] as Set,
                [] as Set,
                [] as Set,
                [] as Set
        )
        managed.setQuorum("eth_test", "not_empty")
        managed.setQuorum("eth_foo", "not_lagging")
        when:
        def act = managed.createQuorumFor("eth_test")
        then:
        act instanceof NotNullQuorum

        when:
        act = managed.createQuorumFor("eth_foo")
        then:
        act instanceof NotLaggingQuorum

        when:
        act = managed.createQuorumFor("eth_bar")
        then:
        act instanceof AlwaysQuorum
    }

    def "Doesn't reuse same instance"() {
        def managed = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM),
                ["eth_test"] as Set,
                [] as Set,
                [] as Set,
                [] as Set
        )
        def parallel = Executors.newFixedThreadPool(16)
        when:
        List<CallQuorum> instances = []
        50.times {
            instances << managed.createQuorumFor("eth_test")
        }
        parallel.shutdown()
        parallel.awaitTermination(5, TimeUnit.SECONDS)

        def ids = instances.collect { System.identityHashCode(it) }

        then:
        instances.size() == 50
        ids.toSet().size() == 50
    }

    def "Test enable method group"() {
        setup:
        def managed = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM),
                [] as Set,
                [] as Set,
                ["filter"] as Set,
                [] as Set
        )

        when:
        def act = managed.getSupportedMethods()

        then:
        act.containsAll([
                "eth_getFilterChanges",
                "eth_getFilterLogs",
                "eth_uninstallFilter",
                "eth_newFilter",
                "eth_newBlockFilter",
                "eth_newPendingTransactionFilter"
        ])
    }

    def "Test enable method group minus one"() {
        setup:
        def managed = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM),
                [] as Set,
                ["eth_newPendingTransactionFilter"] as Set,
                ["filter"] as Set,
                [] as Set
        )

        when:
        def act = managed.getSupportedMethods()

        then:
        act.containsAll([
                "eth_getFilterChanges",
                "eth_getFilterLogs",
                "eth_uninstallFilter",
                "eth_newFilter",
                "eth_newBlockFilter",
        ])
        !act.contains("eth_newPendingTransactionFilter")
    }

    def "Test disabled group not disable enabled method"() {
        setup:
        def managed = new ManagedCallMethods(
                new DefaultEthereumMethods(Chain.ETHEREUM),
                ["eth_newPendingTransactionFilter"] as Set,
                [] as Set,
                [] as Set,
                ["filter"] as Set
        )

        when:
        def act = managed.getSupportedMethods()

        then:
        with(act.findAll {it in [
                "eth_getFilterChanges",
                "eth_getFilterLogs",
                "eth_uninstallFilter",
                "eth_newFilter",
                "eth_newBlockFilter",
                "eth_newPendingTransactionFilter"
        ]}) {
            size() == 1
            first() == "eth_newPendingTransactionFilter"
        }
    }
}
