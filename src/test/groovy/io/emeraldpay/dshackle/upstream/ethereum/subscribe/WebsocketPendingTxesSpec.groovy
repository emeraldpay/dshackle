/**
 * Copyright (c) 2022 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

class WebsocketPendingTxesSpec extends Specification {

    def "Produces values"() {
        setup:
        def responses = [
                '"0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c"',
                '"0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e"',
                '"0x67f22a3b441ea312306f97694ca8159f8d6faaccf0f5ce6442c84b13991f1d23"',
                '"0xa38173981f8eab96ee70cefe42735af0f574b7ef354565f2fea32a28e5ed9bd2"',
        ].collect { it.bytes }
        def ws = Mock(WsSubscriptions)
        def pending = new WebsocketPendingTxes(ws)

        when:
        def txes = pending.connect(Selector.empty).take(3)
                .collectList().block(Duration.ofSeconds(1))

        then:
        1 * ws.subscribe("newPendingTransactions") >> new WsSubscriptions.SubscribeData(
                Flux.fromIterable(responses), "id"
        )
        txes.collect {it.toHex() } == [
                "0xa61bab14fc9720ea8725622688c2f964666d7c2afdae38af7dad53f12f242d5c",
                "0x911548eb0f3bf353a54e03a3506c7c3e747470d6c201f03babbc07ff6e14cd6e",
                "0x67f22a3b441ea312306f97694ca8159f8d6faaccf0f5ce6442c84b13991f1d23",
        ]
    }
}
