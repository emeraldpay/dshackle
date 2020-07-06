package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.config.TokensConfig
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.test.ReaderMock
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.erc20.ERC20Token
import io.infinitape.etherjar.hex.HexData
import io.infinitape.etherjar.rpc.json.TransactionCallJson
import reactor.core.publisher.Mono
import spock.lang.Specification

class TrackERC20AddressSpec extends Specification {

    def "Init with single token"() {
        setup:
        MultistreamHolder ups = Mock(MultistreamHolder) {
            _ * isAvailable(Chain.ETHEREUM) >> true
        }
        TokensConfig tokens = new TokensConfig([
                new TokensConfig.Token().tap {
                    id = "dai"
                    blockchain = Chain.ETHEREUM
                    name = "DAI"
                    type = TokensConfig.Type.ERC20
                    address = Address.from("0x6B175474E89094C44Da98b954EedeAC495271d0F")
                }
        ])
        TrackERC20Address track = new TrackERC20Address(ups, tokens)
        when:
        track.init()
        def act = track.tokens
        def supportDai = track.isSupported(Chain.ETHEREUM, "DAI")
        def supportSai = track.isSupported(Chain.ETHEREUM, "SAI")

        then:
        act.size() == 1
        with(act.keySet().first()) {
            chain == Chain.ETHEREUM
            name == "dai"
        }
        with(act.values().first()) {
            chain == Chain.ETHEREUM
            name == "dai"
            token != null
        }
        supportDai
        !supportSai
    }

    def "Init without tokens"() {
        setup:
        MultistreamHolder ups = Mock(MultistreamHolder) {
            _ * isAvailable(Chain.ETHEREUM) >> true
        }

        TokensConfig tokens = new TokensConfig([])
        TrackERC20Address track = new TrackERC20Address(ups, tokens)
        when:
        track.init()
        def act = track.tokens
        def supportDai = track.isSupported(Chain.ETHEREUM, "dai")
        def supportSai = track.isSupported(Chain.ETHEREUM, "sai")

        then:
        act.size() == 0
        !supportDai
        !supportSai
    }

    def "Init with two tokens"() {
        setup:
        MultistreamHolder ups = Mock(MultistreamHolder) {
            _ * isAvailable(Chain.ETHEREUM) >> true
        }

        TokensConfig tokens = new TokensConfig([
                new TokensConfig.Token().tap {
                    id = "dai"
                    blockchain = Chain.ETHEREUM
                    name = "DAI"
                    type = TokensConfig.Type.ERC20
                    address = Address.from("0x6B175474E89094C44Da98b954EedeAC495271d0F")
                },
                new TokensConfig.Token().tap {
                    id = "sai"
                    blockchain = Chain.ETHEREUM
                    name = "SAI"
                    type = TokensConfig.Type.ERC20
                    address = Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")
                }
        ])
        TrackERC20Address track = new TrackERC20Address(ups, tokens)
        when:
        track.init()
        def act = track.tokens
        def supportDai = track.isSupported(Chain.ETHEREUM, "dai")
        def supportSai = track.isSupported(Chain.ETHEREUM, "sai")

        then:
        act.size() == 2
        with(act[act.keySet().find { it.name == "dai" }]) {
            chain == Chain.ETHEREUM
            name == "dai"
        }
        with(act[act.keySet().find { it.name == "sai" }]) {
            chain == Chain.ETHEREUM
            name == "sai"
        }
        supportDai
        supportSai
    }

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

        EthereumUpstream upstream = new EthereumUpstreamMock(Chain.ETHEREUM, api)
        MultistreamHolder ups = new MultistreamHolderMock(Chain.ETHEREUM, upstream)
        TrackERC20Address track = new TrackERC20Address(ups, new TokensConfig([]))

        TrackERC20Address.TrackedAddress address = new TrackERC20Address.TrackedAddress(
                Chain.ETHEREUM,
                Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"),
                new ERC20Token(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")),
                "test",
                BigInteger.valueOf(1234)
        )
        when:
        def act = track.getBalance(address).block()

        then:
        act.toLong() == 0x1f28d72868
    }

    def "Builds response"() {
        setup:
        TrackERC20Address track = new TrackERC20Address(Stub(MultistreamHolder), new TokensConfig([]))
        TrackERC20Address.TrackedAddress address = new TrackERC20Address.TrackedAddress(
                Chain.ETHEREUM,
                Address.from("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"),
                new ERC20Token(Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")),
                "test",
                BigInteger.valueOf(1234)
        )
        when:
        def act = track.buildResponse(address)
        then:
        act == BlockchainOuterClass.AddressBalance.newBuilder()
                .setAddress(Common.SingleAddress.newBuilder().setAddress("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
                .setAsset(Common.Asset.newBuilder()
                        .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                        .setCode("TEST")
                )
                .setBalance("1234")
                .build()
    }
}
