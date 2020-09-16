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
package io.emeraldpay.dshackle.rpc

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinReader
import io.emeraldpay.dshackle.upstream.bitcoin.XpubAddresses
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.grpc.Chain
import org.bitcoinj.core.Address
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.params.TestNet3Params
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class TrackBitcoinAddressSpec extends Specification {

    String hash1 = "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22"
    ObjectMapper objectMapper = Global.objectMapper

    def "Correct sum for single"() {
        setup:
        def unspents = [
                new SimpleUnspent("f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef", 0, 100L, 123L)
        ]
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
        def address = new TrackBitcoinAddress.Address(
                Chain.BITCOIN, "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        )
        when:
        def total = track.totalUnspent(address, false, unspents)

        then:
        total.balance == 100
        total.utxo.isEmpty()
    }

    def "Correct sum for few"() {
        setup:
        def unspents = [
                new SimpleUnspent("f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef", 0, 100L, 123L),
                new SimpleUnspent("17d1c4adf14b222e652c58d11435fa9ee2ddea000c6f5e20e6b715eb940fc28f", 0, 123L, 123L),
        ]
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
        def address = new TrackBitcoinAddress.Address(
                Chain.BITCOIN, "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        )
        when:
        def total = track.totalUnspent(address, false, unspents)

        then:
        total.balance == 223
        total.utxo.isEmpty()
    }

    def "Correct sum for few with utxo"() {
        setup:
        def unspents = [
                new SimpleUnspent("f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef", 0, 100L, 123L),
                new SimpleUnspent("17d1c4adf14b222e652c58d11435fa9ee2ddea000c6f5e20e6b715eb940fc28f", 0, 123L, 123L),
        ]
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
        def address = new TrackBitcoinAddress.Address(
                Chain.BITCOIN, "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK"
        )
        when:
        def total = track.totalUnspent(address, true, unspents)

        then:
        total.balance == 223
        total.utxo.size() == 2
        total.utxo[0].txid == "f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef"
        total.utxo[1].txid == "17d1c4adf14b222e652c58d11435fa9ee2ddea000c6f5e20e6b715eb940fc28f"
    }

    def "One address for single provided"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
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
        def act = track.allAddresses(Stub(BitcoinMultistream), req).collectList().block()
        then:
        act == ["16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk"]
    }

    def "Sorted addresses for multiple provided"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
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
        def act = track.allAddresses(Stub(BitcoinMultistream), req).collectList().block()
        then:
        act == ["16rCmCmbuWDhPjWTrpQGaU3EPdZF7MTdUk", "1K7xkspJg7DDKNwzXgoRSDCUxiFsRegsSK", "3BMqADKWoWHPASsUdHvnUL6E1jpZkMnLZz", "bc1qdthqvt6cllzej7uhdddrltdfsmnt7d0gl5ue5n"]
    }

    def "Empty for no address provided"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .build()
        when:
        def act = track.allAddresses(Stub(BitcoinMultistream), req).collectList().block()
        then:
        act == []
    }

    def "Use active for xpub"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAddress(
                        Common.AnyAddress.newBuilder()
                                .setAddressXpub(
                                        // seed: chimney battle code relief era plug finish video patch dream pumpkin govern destroy fresh color
                                        Common.XpubAddress.newBuilder()
                                                .setXpub(ByteString.copyFromUtf8("zpub6tz4F49K5B4m7r7EyBKYM9K44eGECaQ2AfrCybq1w7ALFatz9856vrXxAPSrteDA4d5sjUPW3ACNq8wB2V3ugXVJxvAPAYPAYHsVm3VAncL"))
                                                .setLimit(25)
                                )
                )
                .build()
        def xpubAddresses = Mock(XpubAddresses) {
            1 * activeAddresses(
                    "zpub6tz4F49K5B4m7r7EyBKYM9K44eGECaQ2AfrCybq1w7ALFatz9856vrXxAPSrteDA4d5sjUPW3ACNq8wB2V3ugXVJxvAPAYPAYHsVm3VAncL",
                    0,
                    25
            ) >> Flux.fromIterable([
                    "bc1q25590fu8djhw9lvxxqz8ufjyfwup9h54u8fl6t",
                    "bc1q3k6e6vawd5l5syu9nlxn2xsch9afgunl8dnz94",
                    "bc1qu7hd6wycy686kakfps9c093szufjpwnh6rjs9s"
            ]).map { Address.fromString(MainNetParams.get(), it) }
        }
        def multistream = Mock(BitcoinMultistream) {
            1 * getXpubAddresses() >> xpubAddresses
        }
        when:
        def act = track.allAddresses(multistream, req).collectList().block()
        then:
        act == [
                "bc1q25590fu8djhw9lvxxqz8ufjyfwup9h54u8fl6t",
                "bc1q3k6e6vawd5l5syu9nlxn2xsch9afgunl8dnz94",
                "bc1qu7hd6wycy686kakfps9c093szufjpwnh6rjs9s"
        ]
    }

    def "Build proto for common balance"() {
        setup:
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
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
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
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
        TrackBitcoinAddress track = new TrackBitcoinAddress(Stub(MultistreamHolder))
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

        def blocks = TopicProcessor.create()
        Head head = Mock(Head) {
            1 * getFlux() >> Flux.concat(
                    Flux.just(
                            new BlockContainer(0L, BlockId.from(hash1), BigInteger.ZERO, Instant.now(), false, null, null, [])
                    ),
                    Flux.from(blocks)
            )
        }
        def upstream = null
        upstream = Mock(BitcoinMultistream) {
            _ * getReader() >> Mock(BitcoinReader) {
                2 * listUnspent(_) >>> [
                        Mono.just([]),
                        Mono.just([
                                new SimpleUnspent("f14b222e652c58d11435fa9172ddea000c6f5e20e6b715eb940fc28d1c4adeef", 0, 1230000L, 123L)
                        ])
                ]
            }
            _ * getHead() >> head
            _ * cast(_) >> {
                upstream
            }
        }
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.BITCOIN, upstream)
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
                    blocks.onNext(new BlockContainer(1L, BlockId.from(hash1), BigInteger.ONE, Instant.now(), false, null, null, []))
                }
                .expectNext("1230000")
                .then {
                    blocks.onComplete()
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))

    }
}
