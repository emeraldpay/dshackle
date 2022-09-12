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

import io.emeraldpay.dshackle.test.EthereumPosRpcUpstreamMock
import io.emeraldpay.dshackle.test.EthereumRpcUpstreamMock
import io.emeraldpay.dshackle.test.ReaderMock
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.FilteredApis
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.erc20.ERC20Token
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.rpc.json.TransactionCallJson
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

import java.time.Duration

class ERC20BalanceSpec extends Specification {

    def "Gets balance from upstream"() {
        setup:
        ReaderMock api = new ReaderMock()
                .with(
                        new JsonRpcRequest("eth_call", [
                                new TransactionCallJson().tap { json ->
                                    json.setTo(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"))
                                    json.setData(HexData.from("0x70a0823100000000000000000000000016c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
                                },
                                "latest"
                        ]),
                        JsonRpcResponse.ok('"0x0000000000000000000000000000000000000000000000000000001f28d72868"')
                )

        EthereumPosRpcUpstream upstream = new EthereumPosRpcUpstreamMock(Chain.ETHEREUM, api)
        ERC20Token token = new ERC20Token(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"))
        ERC20Balance query = new ERC20Balance()

        when:
        def act = query.getBalance(upstream, token, Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
                .block(Duration.ofSeconds(1))

        then:
        act.toLong() == 0x1f28d72868
    }

    def "Gets balance from api source"() {
        setup:
        ReaderMock api = new ReaderMock()
                .with(
                        new JsonRpcRequest("eth_call", [
                                new TransactionCallJson().tap { json ->
                                    json.setTo(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"))
                                    json.setData(HexData.from("0x70a0823100000000000000000000000016c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
                                },
                                "latest"
                        ]),
                        JsonRpcResponse.ok('"0x0000000000000000000000000000000000000000000000000000001f28d72868"')
                )

        EthereumPosRpcUpstream upstream = new EthereumPosRpcUpstreamMock(Chain.ETHEREUM, api)
        ERC20Token token = new ERC20Token(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"))
        ERC20Balance query = new ERC20Balance()

        ApiSource apiSource = new FilteredApis(
                Chain.ETHEREUM, [upstream], Selector.empty
        )

        when:
        def act = query.getBalance(apiSource, token, Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
                .block(Duration.ofSeconds(1))

        then:
        act.toLong() == 0x1f28d72868
    }
}
