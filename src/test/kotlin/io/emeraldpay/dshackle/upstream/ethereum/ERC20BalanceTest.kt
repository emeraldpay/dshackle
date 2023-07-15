package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.ReaderMock
import io.emeraldpay.dshackle.upstream.FilteredApis
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.erc20.ERC20Token
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.rpc.json.TransactionCallJson
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import java.time.Duration

class ERC20BalanceTest : ShouldSpec({

    should("Get balance from upstream") {
        val api = ReaderMock<JsonRpcRequest, JsonRpcResponse>()
            .with(
                JsonRpcRequest(
                    "eth_call",
                    listOf(
                        TransactionCallJson().also { json ->
                            json.to = Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")
                            json.data = HexData.from("0x70a0823100000000000000000000000016c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b")
                        },
                        "latest"
                    )
                ),
                JsonRpcResponse.ok("\"0x0000000000000000000000000000000000000000000000000000001f28d72868\"")
            )

        val upstream = EthereumUpstreamMock(api = api)
        val token = ERC20Token(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"))
        val query = ERC20Balance()

        val act = query.getBalance(upstream, token, Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
            .block(Duration.ofSeconds(1))

        act.toLong() shouldBe 0x1f28d72868
    }

    should("Get balance from api source") {
        val api = ReaderMock<JsonRpcRequest, JsonRpcResponse>()
            .with(
                JsonRpcRequest(
                    "eth_call",
                    listOf(
                        TransactionCallJson().also { json ->
                            json.to = Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")
                            json.data = HexData.from("0x70a0823100000000000000000000000016c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b")
                        },
                        "latest"
                    )
                ),
                JsonRpcResponse.ok("\"0x0000000000000000000000000000000000000000000000000000001f28d72868\"")
            )

        val upstream = EthereumUpstreamMock(api = api)
        val token = ERC20Token(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"))
        val query = ERC20Balance()

        val apiSource = FilteredApis(Chain.ETHEREUM, listOf(upstream), Selector.empty)

        val act = query.getBalance(apiSource, token, Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
            .block(Duration.ofSeconds(1))

        act.toLong() shouldBe 0x1f28d72868
    }

    should("Accept zero balance returned from non-existing erc20") {
        val api = ReaderMock<JsonRpcRequest, JsonRpcResponse>()
            .with(
                JsonRpcRequest(
                    "eth_call",
                    listOf(
                        TransactionCallJson().also { json ->
                            json.to = Address.from("0x54eedeac495271d0f6b175474e89094c44da0000")
                            json.data = HexData.from("0x70a0823100000000000000000000000016c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b")
                        },
                        "latest"
                    )
                ),
                JsonRpcResponse.ok("\"0x\"")
            )

        val upstream = EthereumUpstreamMock(api = api)
        val token = ERC20Token(Address.from("0x54eedeac495271d0f6b175474e89094c44da0000"))
        val query = ERC20Balance()

        val act = query.getBalance(upstream, token, Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
            .block(Duration.ofSeconds(1))

        act.toLong() shouldBe 0
    }
})
