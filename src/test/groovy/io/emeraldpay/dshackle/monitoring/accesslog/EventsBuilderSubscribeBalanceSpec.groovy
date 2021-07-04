package io.emeraldpay.dshackle.monitoring.accesslog

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.grpc.Chain
import spock.lang.Specification

class EventsBuilderSubscribeBalanceSpec extends Specification {

    def "Basic ethereum event"() {
        setup:
        def request = BlockchainOuterClass.BalanceRequest.newBuilder()
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
        def resp = BlockchainOuterClass.AddressBalance.newBuilder()
                .setAddress(Common.SingleAddress.newBuilder()
                        .setAddress("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(100)
                        .setCode("ETHER"))
                .setBalance("1234560000000000000")
                .build()
        when:
        def act = new EventsBuilder.SubscribeBalance(true)
                .withRequest(request)
                .onReply(resp)
        then:
        act.index == 0
        act.blockchain == Chain.ETHEREUM
        act.balanceRequest.asset == "ETHER"
        act.balanceRequest.addressType == "ADDRESS_SINGLE"
        act.addressBalance.asset == "ETHER"
        act.addressBalance.address == "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    }

    def "Basic bitcoin event"() {
        setup:
        def request = BlockchainOuterClass.BalanceRequest.newBuilder()
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
        def resp = BlockchainOuterClass.AddressBalance.newBuilder()
                .setAddress(Common.SingleAddress.newBuilder()
                        .setAddress("1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s"))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(1)
                        .setCode("BTC"))
                .setBalance("12345600000000")
                .build()
        when:
        def act = new EventsBuilder.SubscribeBalance(true)
                .withRequest(request)
                .onReply(resp)
        then:
        act.index == 0
        act.blockchain == Chain.BITCOIN
        act.balanceRequest.asset == "BTC"
        act.balanceRequest.addressType == "ADDRESS_SINGLE"
        act.addressBalance.asset == "BTC"
        act.addressBalance.address == "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s"
    }
}
