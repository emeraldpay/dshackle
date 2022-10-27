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

import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import reactor.core.publisher.Mono
import spock.lang.Specification


class EthereumCallSelectorSpec extends Specification {

    def "Get height matcher for latest balance"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "latest"]', head).block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for latest call"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_call", '["0x0000", "latest"]', head).block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for latest storageAt"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getStorageAt", '["0x295a70b2de5e3953354a6a8344e616ed314d7251", "0x0", "latest"]', head).block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for balance on block"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x40"]', head).block()
        then:
        act == new Selector.HeightMatcher(0x40)
    }

    def "Get height matcher for balance on block referred by hash"() {
        setup:
        def heights = Mock(Reader) {
            1 * it.read(BlockId.from("0xc90f1c8c125a4d5b90742f16947bdb1d10516f173fd7fc51223d10499de2a812")) >> Mono.just(8606722L)
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights)
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 9128116
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0xc90f1c8c125a4d5b90742f16947bdb1d10516f173fd7fc51223d10499de2a812"]', head).block()
        then:
        act == new Selector.HeightMatcher(8606722)
    }

    def "No matcher for invalid height"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        // 0x10000000000000000 is too large to be a block
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x10000000000000000"]', head).block()
        then:
        act == null
    }

    def "No matcher for negative height"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "-0x100"]', head).block()
        then:
        act == null
    }

    def "No matcher for negative long"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        // 0x8000000000000000 becomes -9223372036854775808 in converted to long as is, i.e. high bit is set
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x8000000000000000"]', head).block()
        then:
        act == null
    }

    def "No matcher for pending balance"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "pending"]', head).block()
        then:
        act == null
    }

    def "Get height matcher with EIP-1898"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader))
        def head = Stub(Head)
        when:
        def act = callSelector.getMatcher("eth_call", '["0x0000", {"blockNumber": "0x100"}]', head).block()
        then:
        act == new Selector.HeightMatcher(0x100)
    }

    def "Get hash matcher with EIP-1898"() {
        setup:
        def heights = Mock(Reader) {
            1 * it.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.just(12079192L)
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights)
        def head = Stub(Head)
        when:
        def act = callSelector.getMatcher("eth_call",
                '["0x0000", {"blockHash": "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"}]', head)
                .block()
        then:
        act == new Selector.HeightMatcher(12079192)
    }

    def "Match head if hash matcher for unknown hash"() {
        setup:
        def heights = Mock(Reader) {
            1 * it.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.empty()
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights)
        def head = Mock(Head) {
            1 * it.getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_call",
                '["0x0000", {"blockHash": "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"}]', head)
                .block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get same matcher for getFilterChanges method"() {
        setup:
        def callSelector = new EthereumCallSelector(Mock(Reader))
        def head = Mock(Head)

        expect:
        callSelector.getMatcher("eth_getFilterChanges", param, head).block()
                == new Selector.SameNodeMatcher((byte)hash)

        where:
        param | hash
        '["0xff09"]' | 9
        '["0xff"]' | 255
        '[""]' | 0
        '["0x0"]' | 0
    }

    def "Get empty matcher for getFilterChanges method without params"() {
        setup:
        def callSelector = new EthereumCallSelector(Mock(Reader))
        def head = Mock(Head)

        when:
        def act = callSelector.getMatcher("eth_getFilterChanges", "[]", head).block()

        then:
        act == null
    }
}
