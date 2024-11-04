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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.test.GenericUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJson
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class WsConnectionImplSpec extends Specification {
    DefaultUpstream upstream = new GenericUpstreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.api())

    def "Makes a RPC call"() {
        setup:
        def wsf = new WsConnectionPoolFactory(
                "test",
                1,
                new WsConnectionFactory(
                        "test",
                        Chain.ETHEREUM__MAINNET,
                        new URI("http://localhost"),
                        new URI("http://localhost"),
                        Schedulers.boundedElastic()
                )
        )
        def apiMock = TestingCommons.api()
        def wsApiMock = apiMock.asWebsocket()
        def ws = wsf.create(upstream).getConnection() as WsConnectionImpl

        def tx = new TransactionJson().tap {
            hash = TransactionId.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        }
        apiMock.answerOnce("eth_getTransactionByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"], tx)

        when:
        Flux.from(ws.handle(wsApiMock.inbound, wsApiMock.outbound)).subscribe()
        def act = ws.callRpc(new ChainRequest("eth_getTransactionByHash", new ListParams(["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"]), 15, null, null, false, Selector.UpstreamFilter.default))

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.id.asNumber() == 15L && Global.objectMapper.readValue(it.result, TransactionJson) == tx
                }
                .expectComplete()
                .verify(Duration.ofSeconds(5))
    }

    def "Makes a RPC call - return null"() {
        setup:
        def wsf = new WsConnectionPoolFactory(
                "test",
                1,
                new WsConnectionFactory(
                        "test",
                        Chain.ETHEREUM__MAINNET,
                        new URI("http://localhost"),
                        new URI("http://localhost"),
                        Schedulers.boundedElastic()
                )
        )
        def apiMock = TestingCommons.api()
        def wsApiMock = apiMock.asWebsocket()
        def ws = wsf.create(upstream).getConnection()

        apiMock.answerOnce("eth_getTransactionByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"], null)

        when:
        Flux.from(ws.handle(wsApiMock.inbound, wsApiMock.outbound)).subscribe()
        def act = ws.callRpc(new ChainRequest("eth_getTransactionByHash", new ListParams(["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"]), 15, null, null, false, Selector.UpstreamFilter.default))

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.id.asNumber() == 15L &&
                            it.resultAsRawString == 'null'
                }
                .expectComplete()
                .verify(Duration.ofSeconds(5))
    }

    def "Makes a RPC call - return error"() {
        setup:
        def wsf = new WsConnectionPoolFactory(
                "test",
                1,
                new WsConnectionFactory(
                        "test",
                        Chain.ETHEREUM__MAINNET,
                        new URI("http://localhost"),
                        new URI("http://localhost"),
                        Schedulers.boundedElastic()
                )
        )
        def apiMock = TestingCommons.api()
        def wsApiMock = apiMock.asWebsocket()
        def ws = wsf.create(upstream).getConnection()

        apiMock.answerOnce("eth_getTransactionByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"],
                new RpcResponseError(RpcResponseError.CODE_METHOD_NOT_EXIST, "test"))

        when:
        Flux.from(ws.handle(wsApiMock.inbound, wsApiMock.outbound)).subscribe()
        def act = ws.callRpc(new ChainRequest("eth_getTransactionByHash", new ListParams(["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200"]), 15, null, null, false, Selector.UpstreamFilter.default))

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    it.id.asNumber() == 15L &&
                            it.error != null &&
                            it.error.code == RpcResponseError.CODE_METHOD_NOT_EXIST && it.error.message == "test"
                }
                .expectComplete()
                .verify(Duration.ofSeconds(5))
    }
}
