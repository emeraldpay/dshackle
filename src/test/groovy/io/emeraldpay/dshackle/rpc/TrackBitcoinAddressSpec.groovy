package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.bitcoin.DirectBitcoinApi
import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class TrackBitcoinAddressSpec extends Specification {

    String hash1 = "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22"

    def "Correct sum from multiple"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-one-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], unspents)

        then:
        total.size() == 1
        total[0].address.chain == Chain.BITCOIN
        total[0].address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        total[0].balance.toString() == "32928461"
    }

    def "Correct sum when other addresses"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], unspents)

        then:
        total.size() == 1
        total[0].address.chain == Chain.BITCOIN
        total[0].address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        total[0].balance.toString() == "32928461"
    }

    def "Sum for two addresses"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP"], unspents).sort { it.address.address }

        then:
        total.size() == 2
        with(total[0]) {
            address.chain == Chain.BITCOIN
            address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
            balance.toString() == "32928461"
        }
        with(total[1]) {
            address.chain == Chain.BITCOIN
            address.address == "35hK24tcLEWcgNA4JxpvbkNkoAcDGqQPsP"
            balance.toString() == "25550215615737"
        }
    }

    def "Zero for empty unspents"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], [])

        then:
        total.size() == 1
        total[0].address.chain == Chain.BITCOIN
        total[0].address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        total[0].balance.toString() == "0"
    }

    def "Zero for unknown address"() {
        setup:
        def json = this.class.getClassLoader().getResourceAsStream("bitcoin/unspent-two-addr.json")
        def unspents = TestingCommons.objectMapper().readValue(json, List)
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        when:
        def total = track.getTotal(Chain.BITCOIN, ["16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk", "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"], unspents).sort { it.address.address }

        then:
        total.size() == 2
        with(total[0]) {
            address.address == "16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk"
            balance.toString() == "0"
        }
        with(total[1]) {
            address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
            balance.toString() == "32928461"
        }
    }

    def "One address for single provided"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAddress(
                        Common.AnyAddress.newBuilder()
                                .setAddressSingle(
                                        Common.SingleAddress.newBuilder()
                                                .setAddress("16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk")
                                )
                )
                .build()
        when:
        def act = track.allAddresses(req)
        then:
        act == ["16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk"]
    }

    def "Sorted addresses for multiple provided"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAddress(
                        Common.AnyAddress.newBuilder()
                                .setAddressMulti(
                                        Common.MultiAddress.newBuilder()
                                                .addAddresses(Common.SingleAddress.newBuilder().setAddress("16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk"))
                                                .addAddresses(Common.SingleAddress.newBuilder().setAddress("3BMqADKWoWHPASsUdHvnUL6E1jpZkMnLZz"))
                                                .addAddresses(Common.SingleAddress.newBuilder().setAddress("1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"))
                                                .addAddresses(Common.SingleAddress.newBuilder().setAddress("bc1qdthqvt6cllzej7uhdddrltdfsmnt7d0gl5ue5n"))
                                )
                )
                .build()
        when:
        def act = track.allAddresses(req)
        then:
        act == ["16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk", "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", "3BMqADKWoWHPASsUdHvnUL6E1jpZkMnLZz", "bc1qdthqvt6cllzej7uhdddrltdfsmnt7d0gl5ue5n"]
    }

    def "Null for no address provided"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .build()
        when:
        def act = track.allAddresses(req)
        then:
        act == null
    }

    def "Build proto for common balance"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        def balance = new TrackBitcoinAddress.AddressBalance(Chain.BITCOIN, "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", BigInteger.valueOf(123456))
        when:
        def act = track.buildResponse(balance)
        then:
        act.address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        act.balance == "123456"
        act.asset.chain.number == Chain.BITCOIN.id
        act.asset.code == "BTC"
    }

    def "Build proto for zero balance"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        def balance = new TrackBitcoinAddress.AddressBalance(Chain.BITCOIN, "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", BigInteger.ZERO)
        when:
        def act = track.buildResponse(balance)
        then:
        act.address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        act.balance == "0"
        act.asset.chain.number == Chain.BITCOIN.id
        act.asset.code == "BTC"
    }

    def "Build proto for all bitcoins"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(Upstreams))
        def balance = new TrackBitcoinAddress.AddressBalance(Chain.BITCOIN, "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", BigInteger.valueOf(21_000_000).multiply(BigInteger.TEN.pow(8)))
        when:
        def act = track.buildResponse(balance)
        then:
        act.address.address == "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        act.balance == "2100000000000000"
        act.asset.chain.number == Chain.BITCOIN.id
        act.asset.code == "BTC"
    }

    def "Get update for a balance"() {
        setup:

        DirectBitcoinApi api = Mock(DirectBitcoinApi) {
            2 * executeAndResult(0, "listunspent", [], List) >>> [
                    Mono.just([]), Mono.just([[address: "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", amount: 0.0123]])
            ]
        }
        def blocks = TopicProcessor.create()
        Head head = Mock(Head) {
            1 * getFlux() >> Flux.from(blocks)
        }
        Upstream upstream
        upstream = Mock(AggregatedUpstream) {
            _ * getApi(_) >> Mono.just(api)
            _ * getHead() >> head
            _ * castApi(_) >> { return upstream }
        }
        Upstreams upstreams = Mock(Upstreams) {
            _ * getUpstream(Chain.BITCOIN) >> upstream
        }
        TrackBitcoinAddress track = new TrackBitcoinAddress(upstreams)

        when:
        def resp = track.subscribe(BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAsset(Common.Asset.newBuilder().setChain(Common.ChainRef.CHAIN_BITCOIN))
                .setAddress(
                        Common.AnyAddress.newBuilder().setAddressSingle(
                                Common.SingleAddress.newBuilder().setAddress("1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK")
                        )
                )
                .build()
        ).map {
            it.balance
        }

        then:
        StepVerifier.create(resp)
                .expectNext("0")
                .then {
                    blocks.onNext(new BlockContainer(1L, BlockId.from(hash1), BigInteger.ONE, Instant.now(), false, null, []))
                }
                .expectNext("1230000")
                .then {
                    blocks.onComplete()
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))

    }
}
