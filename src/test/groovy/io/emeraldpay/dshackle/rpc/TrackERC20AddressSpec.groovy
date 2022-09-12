package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.config.TokensConfig
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.ERC20Balance
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumSubscribe
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectLogs
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.LogMessage
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.Hex32
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.erc20.ERC20Token
import io.emeraldpay.etherjar.hex.HexData
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

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

    def "Check balance when event happens"() {
        setup:
        def events = [
                new LogMessage(
                        Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9"),
                        BlockHash.from("0x0c0d2969c843d0b61fbab1b2302cf24d6681b2ae0a140a3c2908990d048f7631"),
                        13668750,
                        HexData.from("0x0000000000000000000000000000000000000000000000000000000048f2fc7b"),
                        1,
                        [
                                Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
                                Hex32.from("0x000000000000000000000000b02f1329d6a6acef07a763258f8509c2847a0a3e"),
                                Hex32.from("0x00000000000000000000000016c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b")
                        ],
                        TransactionId.from("0x5a7898e27120575c33d3d0179af3b6353c7268bbad4255df079ed26b743a21a5"),
                        1,
                        false
                )
        ]
        def logs = Mock(ConnectLogs) {
            1 * start(
                    [Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")],
                    [Hex32.from("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")],
                    Selector.empty
            ) >> { args ->
                println("ConnectLogs.start $args")
                Flux.fromIterable(events)
            }
        }
        def sub = Mock(EthereumSubscribe) {
            1 * getLogs() >> logs
        }
        def up = Mock(EthereumMultistream) {
            1 * getSubscribe() >> sub
            _ * cast(EthereumMultistream) >> { args ->
                it
            }
        }
        def mup = Mock(MultistreamHolder) {
            _ * getUpstream(Chain.ETHEREUM) >> up
        }
        TokensConfig tokens = new TokensConfig([
                new TokensConfig.Token().tap {
                    id = "test"
                    blockchain = Chain.ETHEREUM
                    name = "TEST"
                    type = TokensConfig.Type.ERC20
                    address = Address.from("0x54EedeAC495271d0F6B175474E89094C44Da98b9")
                }
        ])
        TrackERC20Address track = new TrackERC20Address(mup, tokens)
        track.init()
        track.erc20Balance = Mock(ERC20Balance) {
            2 * it.getBalance(_, _, _) >>> [
                    Mono.just(100000.toBigInteger()),
                    Mono.just(150000.toBigInteger())
            ]
        }
        def request = BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAddress(
                        Common.AnyAddress.newBuilder()
                                .setAddressSingle(Common.SingleAddress.newBuilder().setAddress("0x16c15c65ad00b6dfbcc2cb8a7b6c2d0103a3883b"))
                )
                .setAsset(
                        Common.Asset.newBuilder()
                                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                                .setCode("TEST")
                )
                .build()
        when:
        def act = track.subscribe(request)

        then:
        StepVerifier.create(act)
                .expectNextMatches {
                    println("Received: $it")
                    it.getBalance() == "100000"
                }
                .expectNextMatches {
                    println("Received: $it")
                    it.getBalance() == "150000"
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
