package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import java.util.UUID

class AccessRecordBuilderSubscribeBalanceTest : ShouldSpec({

    should("Process basic ethereum event") {
        val request = BlockchainOuterClass.BalanceRequest.newBuilder()
            .setAddress(
                Common.AnyAddress.newBuilder()
                    .setAddressSingle(
                        Common.SingleAddress.newBuilder()
                            .setAddress("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
                    )
            )
            .setAsset(
                Common.Asset.newBuilder()
                    .setChainValue(100)
                    .setCode("ETHER")
            )
            .build()

        val resp = BlockchainOuterClass.AddressBalance.newBuilder()
            .setAddress(
                Common.SingleAddress.newBuilder()
                    .setAddress("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
            )
            .setAsset(
                Common.Asset.newBuilder()
                    .setChainValue(100)
                    .setCode("ETHER")
            )
            .setBalance("1234560000000000000")
            .build()

        val act = RecordBuilder.SubscribeBalance(true, UUID.randomUUID()).also {
            it.onRequest(request)
        }.onReply(resp)

        act.index shouldBe 0
        act.blockchain shouldBe Chain.ETHEREUM
        act.balanceRequest.asset shouldBe "ETHER"
        act.balanceRequest.addressType shouldBe "ADDRESS_SINGLE"
        act.addressBalance.asset shouldBe "ETHER"
        act.addressBalance.address shouldBe "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    }

    should("Process basic bitcoin event") {

        val request = BlockchainOuterClass.BalanceRequest.newBuilder()
            .setAddress(
                Common.AnyAddress.newBuilder()
                    .setAddressSingle(
                        Common.SingleAddress.newBuilder()
                            .setAddress("1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s")
                    )
            )
            .setAsset(
                Common.Asset.newBuilder()
                    .setChainValue(1)
                    .setCode("BTC")
            )
            .build()
        val resp = BlockchainOuterClass.AddressBalance.newBuilder()
            .setAddress(
                Common.SingleAddress.newBuilder()
                    .setAddress("1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s")
            )
            .setAsset(
                Common.Asset.newBuilder()
                    .setChainValue(1)
                    .setCode("BTC")
            )
            .setBalance("12345600000000")
            .build()

        val act = RecordBuilder.SubscribeBalance(true, UUID.randomUUID()).also {
            it.onRequest(request)
        }.onReply(resp)

        act.index shouldBe 0
        act.blockchain shouldBe Chain.BITCOIN
        act.balanceRequest.asset shouldBe "BTC"
        act.balanceRequest.addressType shouldBe "ADDRESS_SINGLE"
        act.addressBalance.asset shouldBe "BTC"
        act.addressBalance.address shouldBe "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s"
    }
})
