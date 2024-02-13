/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum


import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.generic.GenericMultistream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.etherjar.hex.Hex32
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import spock.lang.Specification

class EthereumEgressSubscriptionSpec extends Specification {

    def "read empty logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([:])

        then:
        act.address == []
        act.topics == []
    }

    def "read single address logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                address: "0x829bd824b016326a401d083b33d092293333a830"
        ])

        then:
        act.address == [
                Address.from("0x829bd824b016326a401d083b33d092293333a830")
        ]
        act.topics == []

        when:
        act = ethereumSubscribe.readLogsRequest([
                address: ["0x829bd824b016326a401d083b33d092293333a830"]
        ])
        then:
        act.address == [
                Address.from("0x829bd824b016326a401d083b33d092293333a830")
        ]
        act.topics == []
    }

    def "ignores invalid address for logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                address: "829bd824b016326a401d083b33d092293333a830"
        ])

        then:
        act.address == []
        act.topics == []
    }

    def "read multi address logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                address: ["0x829bd824b016326a401d083b33d092293333a830", "0x401d083b33d092293333a83829bd824b016326a0"]
        ])

        then:
        act.address == [
                Address.from("0x829bd824b016326a401d083b33d092293333a830"),
                Address.from("0x401d083b33d092293333a83829bd824b016326a0")
        ]
        act.topics == []
    }

    def "read single topic logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                topics: "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        ])

        then:
        act.address == []
        act.topics == [
                Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
        ]

        when:
        act = ethereumSubscribe.readLogsRequest([
                topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
        ])
        then:
        act.address == []
        act.topics == [
                Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
        ]
    }

    def "read invalid topic for request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                topics: [
                        "0x401d083b33d092293333a83829bd824b016326a0",
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                ]
        ])

        then:
        act.address == []
        act.topics == [
                Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
        ]
    }

    def "read multi topic logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                topics: [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
                ]
        ])

        then:
        act.address == []
        act.topics == [
                Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
                Hex32.from("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
        ]
    }

    def "read full logs request"() {
        setup:
        def ethereumSubscribe = new EthereumEgressSubscription(TestingCommons.emptyMultistream() as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        when:
        def act = ethereumSubscribe.readLogsRequest([
                address: "0x298d492e8c1d909d3f63bc4a36c66c64acb3d695",
                topics : [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
                ]
        ])

        then:
        act.address == [
                Address.from("0x298d492e8c1d909d3f63bc4a36c66c64acb3d695")
        ]
        act.topics == [
                Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
                Hex32.from("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
        ]
    }

    def "get available subscriptions"() {
        when:
        def up1 = TestingCommons.upstream("test")
        up1.getConnectorMock().setLiveness(Flux.just(false))

        def ethereumSubscribe1 = new EthereumEgressSubscription(TestingCommons.multistream(up1) as GenericMultistream, Schedulers.boundedElastic(), null)
        then:
        ethereumSubscribe1.getAvailableTopics() == []
        when:
        def up2 = TestingCommons.upstream("test")
        up2.getConnectorMock().setLiveness(Flux.just(true))
        up2.stop()
        up2.start()
        def ethereumSubscribe2 = new EthereumEgressSubscription(TestingCommons.multistream(up2) as GenericMultistream, Schedulers.boundedElastic(), null)
        then:
        ethereumSubscribe2.getAvailableTopics().toSet() == [EthereumEgressSubscription.METHOD_LOGS, EthereumEgressSubscription.METHOD_NEW_HEADS].toSet()
        when:
        def up3 = TestingCommons.upstream("test")
        up3.getConnectorMock().setLiveness(Flux.just(true))
        up3.stop()
        up3.start()
        def ethereumSubscribe3 = new EthereumEgressSubscription(TestingCommons.multistream(up3) as GenericMultistream, Schedulers.boundedElastic(), Stub(PendingTxesSource))
        then:
        ethereumSubscribe3.getAvailableTopics().toSet() == [EthereumEgressSubscription.METHOD_LOGS, EthereumEgressSubscription.METHOD_NEW_HEADS, EthereumEgressSubscription.METHOD_PENDING_TXES].toSet()

    }
}
