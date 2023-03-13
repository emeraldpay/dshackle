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

import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class EthereumCallSelectorSpec extends Specification {

    def "Get height matcher for latest balance"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "latest"]', head, false).block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for latest call"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_call", '["0x0000", "latest"]', head, false).block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for latest storageAt"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getStorageAt", '["0x295a70b2de5e3953354a6a8344e616ed314d7251", "0x0", "latest"]', head, false).block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get height matcher for balance on block"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x40"]', head, false).block()
        then:
        act == new Selector.HeightMatcher(0x40)
    }

    def "Get height matcher for balance on block referred by hash"() {
        setup:
        def heights = Mock(Reader) {
            1 * it.read(BlockId.from("0xc90f1c8c125a4d5b90742f16947bdb1d10516f173fd7fc51223d10499de2a812")) >> Mono.just(8606722L)
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights, Stub(Caches))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 9128116
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0xc90f1c8c125a4d5b90742f16947bdb1d10516f173fd7fc51223d10499de2a812"]', head, false).block()
        then:
        act == new Selector.HeightMatcher(8606722)
    }

    def "No matcher for invalid height"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        // 0x10000000000000000 is too large to be a block
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x10000000000000000"]', head, false).block()
        then:
        act == null
    }

    def "No matcher for negative height"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "-0x100"]', head, false).block()
        then:
        act == null
    }

    def "No matcher for negative long"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        // 0x8000000000000000 becomes -9223372036854775808 in converted to long as is, i.e. high bit is set
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "0x8000000000000000"]', head, false).block()
        then:
        act == null
    }

    def "No matcher for pending balance"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_getBalance", '["0x0000", "pending"]', head, false).block()
        then:
        act == null
    }

    def "Get height matcher with EIP-1898"() {
        setup:
        EthereumCallSelector callSelector = new EthereumCallSelector(Stub(Reader), Stub(Caches))
        def head = Stub(Head)
        when:
        def act = callSelector.getMatcher("eth_call", '["0x0000", {"blockNumber": "0x100"}]', head, false).block()
        then:
        act == new Selector.HeightMatcher(0x100)
    }

    def "Get hash matcher with EIP-1898"() {
        setup:
        def heights = Mock(Reader) {
            1 * it.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.just(12079192L)
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights, Stub(Caches))
        def head = Stub(Head)
        when:
        def act = callSelector.getMatcher("eth_call",
                '["0x0000", {"blockHash": "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"}]', head, false)
                .block()
        then:
        act == new Selector.HeightMatcher(12079192)
    }

    def "Get empty matcher for block tag with passthrough arg"() {
        setup:
        def heights = Mock(Reader) {
            0 * it.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.just(12079192L)
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights, Stub(Caches))
        def head = Stub(Head)
        when:
        def act = callSelector.getMatcher("eth_call",
                '["0x0000", {"blockHash": "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"}]', head, true)
                .block()
        then:
        act == null
    }

    def "Match head if hash matcher for unknown hash"() {
        setup:
        def heights = Mock(Reader) {
            1 * it.read(BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32")) >> Mono.empty()
        }
        EthereumCallSelector callSelector = new EthereumCallSelector(heights, Stub(Caches))
        def head = Mock(Head) {
            1 * it.getCurrentHeight() >> 100
        }
        when:
        def act = callSelector.getMatcher("eth_call",
                '["0x0000", {"blockHash": "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"}]', head, false)
                .block()
        then:
        act == new Selector.HeightMatcher(100)
    }

    def "Get same matcher for getFilterChanges method"() {
        setup:
        def callSelector = new EthereumCallSelector(Mock(Reader), Stub(Caches))
        def head = Mock(Head)

        expect:
        callSelector.getMatcher("eth_getFilterChanges", param, head, true).block()
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
        def callSelector = new EthereumCallSelector(Mock(Reader), Stub(Caches))
        def head = Mock(Head)

        when:
        def act = callSelector.getMatcher("eth_getFilterChanges", "[]", head, false).block()

        then:
        act == null
    }

    def "Get height matcher for getByHash and getTransactionByBlockHash methods"() {
        setup:
        def hash = "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"
        def block = new BlockContainer(
                12079192L, BlockId.from(hash),
                BigInteger.ONE, Instant.now(), false, "".bytes, null, [], 0, "upstream"
        )
        def blockByHashCache = Mock(BlocksMemCache) {
            1 * read(BlockId.from(hash)) >> Mono.just(block)
        }
        def cache = Caches.newBuilder().setBlockByHash(blockByHashCache).build()
        def callSelector = new EthereumCallSelector(Stub(Reader), cache)
        def head = Stub(Head)

        when:
        def act = callSelector.getMatcher(
                method, '["0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32", false]',
                head, false
        )

        then:
        StepVerifier.create(act)
                .expectNext(new Selector.HeightMatcher(12079192L))
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        where:
        method << ["eth_getTransactionByBlockHashAndIndex", "eth_getBlockByHash"]
    }

    def "Get height matcher for getByNumber and getTransactionByBlockNumber methods"() {
        setup:
        def cache = Stub(Caches)
        def callSelector = new EthereumCallSelector(Stub(Reader), cache)
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 17654321L
        }

        when:
        def act = callSelector.getMatcher(
                method, params, head, false
        )

        then:
        StepVerifier.create(act)
                .expectNext(new Selector.HeightMatcher(height))
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        where:
        method | params | height
        "eth_getTransactionByBlockNumberAndIndex" | '["0xfbfe3b", false]' | 16514619L
        "eth_getTransactionByBlockNumberAndIndex" | '["earliest", false]' | 0L
        "eth_getTransactionByBlockNumberAndIndex" | '["latest", false]' | 17654321L
        "eth_getBlockByNumber" | '["0xfbfe3b", false]' | 16514619L
        "eth_getBlockByNumber" | '["earliest", false]' | 0L
        "eth_getBlockByNumber" | '["latest", false]' | 17654321L
    }

    def "No height matcher for getByHash method"() {
        setup:
        def hash = "0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"
        def blockByHashCache = Mock(BlocksMemCache) {
            1 * read(BlockId.from(hash)) >> resultFromCache
        }
        def cache = Caches.newBuilder().setBlockByHash(blockByHashCache).build()
        def callSelector = new EthereumCallSelector(Stub(Reader), cache)
        def head = Stub(Head)

        when:
        def act = callSelector.getMatcher(
                "eth_getBlockByHash", '["0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32", false]',
                head, false
        )

        then:
        StepVerifier.create(act)
                .expectNext()
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        where:
        resultFromCache << [Mono.empty(), Mono.error(new RuntimeException())]
    }

    def "Get height matcher for getLogs method"() {
        setup:
        def cache = Stub(Caches)
        def callSelector = new EthereumCallSelector(Stub(Reader), cache)
        def head = Mock(Head) {
            _ * getCurrentHeight() >> 17654321L
        }

        when:
        def act = callSelector.getMatcher(method, param, head, false)

        then:
        StepVerifier.create(act)
                .expectNext(new Selector.HeightMatcher(height))
                .expectComplete()
                .verify(Duration.ofSeconds(1))

        where:
        method | param | height
        "eth_getLogs" | '[{"toBlock":"0xfbfe3b"}]' | 16514619L
        "eth_getLogs" | '[{"toBlock":"latest"}]' | 17654321L
        "eth_getLogs" | '[{"toBlock":"earliest"}]' | 0L
    }
}
