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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsMessage
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class WsSubscriptionsImplSpec extends Specification {

    def "Makes a subscription"() {
        setup:
        def answers = Flux.fromIterable(
                [
                    new JsonRpcWsMessage("100".bytes, null, "0xcff45d00e7"),
                    new JsonRpcWsMessage("101".bytes, null, "0xcff45d00e7"),
                    new JsonRpcWsMessage("102".bytes, null, "0xcff45d00e7"),
                ]
        )

        def conn = Mock(WsConnection) {
            1 * it.connectionId() >> "id"
        }
        def pool = Mock(WsConnectionPool) {
            getConnection() >> conn
        }
        def ws = new WsSubscriptionsImpl(pool)

        when:
        def act = ws.subscribe(new JsonRpcRequest("eth_subscribe", ["foo_bar"]))
            .data
            .map { new String(it) }
            .take(3)
            .collectList().block(Duration.ofSeconds(1))

        then:
        act == ["100", "101", "102"]

        1 * conn.callRpc({ JsonRpcRequest req ->
            req.method == "eth_subscribe" && req.params == ["foo_bar"]
        }) >> Mono.just(new JsonRpcResponse('"0xcff45d00e7"'.bytes, null))
        1 * conn.getSubscribeResponses() >> answers
    }

    def "Produces only messages to the actual subscription"() {
        setup:
        def answers = Flux.fromIterable(
                [
                        new JsonRpcWsMessage("100".bytes, null, "0xcff45d00e7"),
                        new JsonRpcWsMessage("AAA".bytes, null, "0x000001a0e7"),
                        new JsonRpcWsMessage("101".bytes, null, "0xcff45d00e7"),
                        new JsonRpcWsMessage("BBB".bytes, null, "0x000001a0e7"),
                        new JsonRpcWsMessage("CCC".bytes, null, "0x000001a0e7"),
                        new JsonRpcWsMessage("102".bytes, null, "0xcff45d00e7"),
                        new JsonRpcWsMessage("DDD".bytes, null, "0x000001a0e7"),
                ]
        )

        def conn = Mock(WsConnection) {
            1 * it.connectionId() >> "id"
        }
        def pool = Mock(WsConnectionPool) {
            getConnection() >> conn
        }
        def ws = new WsSubscriptionsImpl(pool)

        when:
        def act = ws.subscribe(new JsonRpcRequest("eth_subscribe", ["foo_bar"]))
                .data
                .map { new String(it) }
                .take(3)
                .collectList().block(Duration.ofSeconds(1))

        then:
        act == ["100", "101", "102"]

        1 * conn.callRpc({ JsonRpcRequest req ->
            req.method == "eth_subscribe" && req.params == ["foo_bar"]
        }) >> Mono.just(new JsonRpcResponse('"0xcff45d00e7"'.bytes, null))
        1 * conn.getSubscribeResponses() >> answers
    }
}
