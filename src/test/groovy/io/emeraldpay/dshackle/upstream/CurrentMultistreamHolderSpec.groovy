/**
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
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.test.GenericUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import spock.lang.Specification

class CurrentMultistreamHolderSpec extends Specification {

    def "add upstream"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.defaultMultistreams())
        def up = new GenericUpstreamMock("test", Chain.ETHEREUM__MAINNET, TestingCommons.api())
        when:
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up, UpstreamChangeEvent.ChangeType.ADDED))
        then:
        current.getAvailable() == [Chain.ETHEREUM__MAINNET]
        current.getUpstream(Chain.ETHEREUM__MAINNET).getAll()[0] == up
    }

    def "add multiple upstreams"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.defaultMultistreams())
        def up1 = new GenericUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api())
        def up2 = new GenericUpstreamMock("test2", Chain.ETHEREUM_CLASSIC__MAINNET, TestingCommons.api())
        def up3 = new GenericUpstreamMock("test3", Chain.ETHEREUM__MAINNET, TestingCommons.api())
        when:
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up1, UpstreamChangeEvent.ChangeType.ADDED))
        current.getUpstream(Chain.ETHEREUM_CLASSIC__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM_CLASSIC__MAINNET, up2, UpstreamChangeEvent.ChangeType.ADDED))
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up3, UpstreamChangeEvent.ChangeType.ADDED))
        current.getUpstream(Chain.ETHEREUM_CLASSIC__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up3, UpstreamChangeEvent.ChangeType.ADDED))
        then:
        current.getAvailable().toSet() == [Chain.ETHEREUM__MAINNET, Chain.ETHEREUM_CLASSIC__MAINNET].toSet()
        current.getUpstream(Chain.ETHEREUM__MAINNET).getAll().toSet() == [up1, up3].toSet()
        current.getUpstream(Chain.ETHEREUM_CLASSIC__MAINNET).getAll().toSet() == [up2].toSet()
    }

    def "remove upstream"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.defaultMultistreams())
        def up1 = new GenericUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api())
        def up2 = new GenericUpstreamMock("test2", Chain.ETHEREUM_CLASSIC__MAINNET, TestingCommons.api())
        def up3 = new GenericUpstreamMock("test3", Chain.ETHEREUM__MAINNET, TestingCommons.api())
        def up1_del = new GenericUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api())
        when:
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up1, UpstreamChangeEvent.ChangeType.ADDED))
        current.getUpstream(Chain.ETHEREUM_CLASSIC__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM_CLASSIC__MAINNET, up2, UpstreamChangeEvent.ChangeType.ADDED))
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up3, UpstreamChangeEvent.ChangeType.ADDED))
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up1_del, UpstreamChangeEvent.ChangeType.REMOVED))
        then:
        current.getAvailable().toSet() == [Chain.ETHEREUM__MAINNET, Chain.ETHEREUM_CLASSIC__MAINNET].toSet()
        current.getUpstream(Chain.ETHEREUM__MAINNET).getAll().toSet() == [up3].toSet()
        current.getUpstream(Chain.ETHEREUM_CLASSIC__MAINNET).getAll().toSet() == [up2].toSet()
    }

    def "available after adding"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.defaultMultistreams())
        def up1 = new GenericUpstreamMock("test1", Chain.ETHEREUM__MAINNET, TestingCommons.api())

        when:
        def act = current.isAvailable(Chain.ETHEREUM__MAINNET)
        then:
        !act

        when:
        current.getUpstream(Chain.ETHEREUM__MAINNET).onUpstreamChange(new UpstreamChangeEvent(Chain.ETHEREUM__MAINNET, up1, UpstreamChangeEvent.ChangeType.ADDED))
        act = current.isAvailable(Chain.ETHEREUM__MAINNET)

        then:
        act
    }
}
