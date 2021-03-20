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
package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import spock.lang.Specification

class EthereumCallSelectorSpec extends Specification {

    EthereumCallSelector callSelector = new EthereumCallSelector()

    def "Get height matcher for latest balance"() {
        setup:
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "latest"]', head)
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for latest call"() {
        setup:
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_call", '["0x0000", "latest"]', head)
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for latest storageAt"() {
        setup:
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getStorageAt", '["0x295a70b2de5e3953354a6a8344e616ed314d7251", "0x0", "latest"]', head)
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for balance on block"() {
        setup:
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x40"]', head)
        then:
        act == new Selector.HeightMatcher(0x40)
    }

    def "No matcher for pending balance"() {
        setup:
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "pending"]', head)
        then:
        act == null
    }

    def "Get height matcher with EIP-1898"() {
        setup:
        def head = Stub(Head)
        when:
        def act = callSelector.getMatcher("eth_call", '["0x0000", {"blockNumber": "0x100"}]', head)
        then:
        act == new Selector.HeightMatcher(0x100)
    }
}
