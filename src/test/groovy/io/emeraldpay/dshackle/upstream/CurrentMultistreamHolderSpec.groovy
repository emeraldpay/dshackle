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

import io.emeraldpay.dshackle.startup.UpstreamChange
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.signature.NoSigner
import io.emeraldpay.api.Chain
import spock.lang.Specification

class CurrentMultistreamHolderSpec extends Specification {

    def "add upstream"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.emptyCaches(), new NoSigner())
        def up = new EthereumUpstreamMock("test", Chain.ETHEREUM, TestingCommons.standardApi())
        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up, UpstreamChange.ChangeType.ADDED))
        then:
        current.getAvailable() == [Chain.ETHEREUM]
        current.getUpstream(Chain.ETHEREUM).getAll()[0] == up
    }

    def "add multiple upstreams"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.emptyCaches(), new NoSigner())
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.standardApi())
        def up2 = new EthereumUpstreamMock("test2", Chain.ETHEREUM_CLASSIC, TestingCommons.standardApi())
        def up3 = new EthereumUpstreamMock("test3", Chain.ETHEREUM, TestingCommons.standardApi())
        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up1, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM_CLASSIC, up2, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM, up3, UpstreamChange.ChangeType.ADDED))
        then:
        current.getAvailable().toSet() == [Chain.ETHEREUM, Chain.ETHEREUM_CLASSIC].toSet()
        current.getUpstream(Chain.ETHEREUM).getAll().toSet() == [up1, up3].toSet()
        current.getUpstream(Chain.ETHEREUM_CLASSIC).getAll().toSet() == [up2].toSet()
    }

    def "remove upstream"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.emptyCaches(), new NoSigner())
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.standardApi())
        def up2 = new EthereumUpstreamMock("test2", Chain.ETHEREUM_CLASSIC, TestingCommons.standardApi())
        def up3 = new EthereumUpstreamMock("test3", Chain.ETHEREUM, TestingCommons.standardApi())
        def up1_del = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.standardApi())
        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up1, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM_CLASSIC, up2, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM, up3, UpstreamChange.ChangeType.ADDED))
        current.update(new UpstreamChange(Chain.ETHEREUM, up1_del, UpstreamChange.ChangeType.REMOVED))
        then:
        current.getAvailable().toSet() == [Chain.ETHEREUM, Chain.ETHEREUM_CLASSIC].toSet()
        current.getUpstream(Chain.ETHEREUM).getAll().toSet() == [up3].toSet()
        current.getUpstream(Chain.ETHEREUM_CLASSIC).getAll().toSet() == [up2].toSet()
    }

    def "available after adding"() {
        setup:
        def current = new CurrentMultistreamHolder(TestingCommons.emptyCaches(), new NoSigner())
        def up1 = new EthereumUpstreamMock("test1", Chain.ETHEREUM, TestingCommons.standardApi())

        when:
        def act = current.isAvailable(Chain.ETHEREUM)
        then:
        !act

        when:
        current.update(new UpstreamChange(Chain.ETHEREUM, up1, UpstreamChange.ChangeType.ADDED))
        act = current.isAvailable(Chain.ETHEREUM)

        then:
        act
    }
}
