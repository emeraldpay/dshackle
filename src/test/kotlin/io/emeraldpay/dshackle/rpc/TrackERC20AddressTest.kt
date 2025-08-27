package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass.AddressBalance
import io.emeraldpay.api.proto.BlockchainOuterClass.BalanceRequest
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.config.TokensConfig
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.ethereum.ERC20Balance
import io.emeraldpay.dshackle.upstream.ethereum.EthereumEgressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectLogs
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.LogMessage
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.EventId
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.erc20.ERC20Token
import io.emeraldpay.etherjar.hex.Hex32
import io.emeraldpay.etherjar.hex.HexData
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger

private const val USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7" // Tether: USDT Stablecoin

class TrackERC20AddressTest :
    ShouldSpec({

        should("supports any ethereum address") {
            val trackERC20Address = trackERC20AddressWithEmptyConfig()
            val request = balanceRequest("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
            val actual = trackERC20Address.isSupported(request)
            actual shouldBe true
        }

        should("track by contract address") {
            val trackERC20Address = trackERC20AddressWithEmptyConfig()
            val erc20Balance = mockkErc20Balance("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "1234560000000000000000")
            trackERC20Address.erc20Balance = erc20Balance

            val request = balanceRequest("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")

            val balance =
                trackERC20Address
                    .getBalance(request)
                    .single()
                    .block()

            balance shouldBe balanceResponse("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "1234560000000000000000")
        }

        should("subscribe by contract address") {
            val subscriptionConnect = mockk<SubscriptionConnect<LogMessage>>()
            every { subscriptionConnect.connect() } returns
                Flux.just(
                    LogMessage(
                        address = Address.from(USDT),
                        blockHash = BlockHash.from("0x0c0d2969c843d0b61fbab1b2302cf24d6681b2ae0a140a3c2908990d048f7631"), // no matter
                        blockNumber = 13668750, // no matter
                        data = HexData.from("0x000000000000000000000000000000000000000000000042ecf6330552400000"), // 1234560000000000000000
                        logIndex = 1,
                        topics =
                            listOf(
                                EventId.fromSignature("Transfer", "address", "address", "uint256"),
                                Hex32.from("0x000000000000000000000000b02f1329d6a6acef07a763258f8509c2847a0a3e"), // no matter
                                Hex32.from("0x0000000000000000000000007a250d5630B4cF539739dF2C5dAcb4c659F2488D"),
                            ),
                        transactionHash = TransactionId.empty(), // no matter
                        transactionIndex = 1,
                        removed = false,
                    ),
                )
            val logs = mockk<ConnectLogs>()
            every {
                logs.create(
                    listOf(Address.from(USDT)),
                    listOf(EventId.fromSignature("Transfer", "address", "address", "uint256")),
                )
            } returns
                subscriptionConnect

            val subscription = mockk<EthereumEgressSubscription>()
            every { subscription.logs } returns logs

            val trackERC20Address =
                trackERC20AddressWithEmptyConfig { upstream ->
                    upstream.apply {
                        every { getEgressSubscription() } returns subscription
                    }
                }
            val erc20Balance = mockkErc20Balance("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "1234560000000000000000")
            trackERC20Address.erc20Balance = erc20Balance

            val request = balanceRequest("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")

            val balance =
                trackERC20Address
                    .subscribe(request)
                    .blockFirst()

            balance shouldBe balanceResponse("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D", "1234560000000000000000")
        }
    })

private fun mockkErc20Balance(
    address: String,
    balance: String,
): ERC20Balance {
    val erc20Balance = mockk<ERC20Balance>()
    every {
        erc20Balance.getBalance(
            any<EthereumMultistream>(),
            ERC20Token(Address.from(USDT)),
            Address.from(address),
        )
    } returns Mono.just(BigInteger(balance))
    return erc20Balance
}

private fun balanceResponse(
    address: String,
    balance: String,
): AddressBalance? =
    AddressBalance
        .newBuilder()
        .setAddress(
            Common.SingleAddress
                .newBuilder()
                .setAddress(address.lowercase()),
        ).setErc20Asset(
            Common.Erc20Asset
                .newBuilder()
                .setChainValue(Chain.ETHEREUM.id)
                .setContractAddress(USDT),
        ).setBalance(balance)
        .build()

private fun balanceRequest(address: String): BalanceRequest =
    BalanceRequest
        .newBuilder()
        .setAddress(
            Common.AnyAddress
                .newBuilder()
                .setAddressSingle(
                    Common.SingleAddress
                        .newBuilder()
                        .setAddress(address),
                ),
        ).setErc20Asset(
            Common.Erc20Asset
                .newBuilder()
                .setChainValue(Chain.ETHEREUM.id)
                .setContractAddress(USDT),
        ).build()

private fun trackERC20AddressWithEmptyConfig(
    upstreamInitializer: (EthereumMultistream) -> EthereumMultistream = {
        it
    },
): TrackERC20Address {
    var upstream = mockk<EthereumMultistream>()
    every { upstream.cast(EthereumMultistream::class.java) } returns upstream
    every { upstream.getHead() } returns EmptyHead()
    upstream = upstreamInitializer(upstream)

    val holder = mockk<MultistreamHolder>()
    every { holder.isAvailable(Chain.ETHEREUM) } returns true
    every { holder.getUpstream(Chain.ETHEREUM) } returns upstream

    val trackERC20Address = TrackERC20Address(holder, TokensConfig(listOf()))
    trackERC20Address.init()
    return trackERC20Address
}
