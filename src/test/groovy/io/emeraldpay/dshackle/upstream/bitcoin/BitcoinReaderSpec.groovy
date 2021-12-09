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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.test.ApiReaderMock
import io.emeraldpay.dshackle.upstream.Head
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class BitcoinReaderSpec extends Specification {

    def "gets block by height"() {
        setup:
        def block = [
                hash: "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506",
                tx  : []
        ]
        def api = new ApiReaderMock()
        api.answerOnce("getblockhash", [100000], "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506")
        api.answerOnce("getblock", ["000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"], block)
        def ups = Mock(BitcoinMultistream) {
            _ * it.getDirectApi(_) >> Mono.just(api)
        }
        def reader = new BitcoinReader(ups, Stub(Head), null)

        when:
        def act = reader.getBlock(100000).block(Duration.ofSeconds(1))

        then:
        act == block
    }
}
