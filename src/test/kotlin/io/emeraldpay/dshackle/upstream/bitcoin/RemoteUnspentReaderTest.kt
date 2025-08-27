package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass.AddressBalance
import io.emeraldpay.api.proto.BlockchainOuterClass.BalanceRequest
import io.emeraldpay.api.proto.BlockchainOuterClass.Utxo
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.grpc.BitcoinGrpcUpstream
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.bitcoinj.core.Address
import org.bitcoinj.params.MainNetParams
import reactor.core.publisher.Flux
import java.time.Duration

class RemoteUnspentReaderTest :
    ShouldSpec({

        should("Create a request") {
            val ups = mockk<BitcoinMultistream>()
            every { ups.chain } returns Chain.BITCOIN
            val reader = RemoteUnspentReader(ups)

            val act = reader.createRequest(Address.fromString(MainNetParams.get(), "38XnPvu9PmonFU9WouPXUjYbW91wa5MerL"))

            act.asset.chain.name shouldBe "CHAIN_BITCOIN"
            act.includeUtxo shouldBe true
            act.address.addrTypeCase shouldBe Common.AnyAddress.AddrTypeCase.ADDRESS_SINGLE
            act.address.addressSingle.address shouldBe "38XnPvu9PmonFU9WouPXUjYbW91wa5MerL"
        }

        should("Read from remote") {
            val ups = mockk<BitcoinMultistream>()
            every { ups.chain } returns Chain.BITCOIN
            val reader = RemoteUnspentReader(ups)

            val up = mockk<BitcoinGrpcUpstream>()
            every { up.remote } returns
                mockk {
                    every { getBalance(any() as BalanceRequest) } returns
                        Flux.fromIterable(
                            listOf(
                                AddressBalance
                                    .newBuilder()
                                    .addAllUtxo(
                                        listOf(
                                            Utxo
                                                .newBuilder()
                                                .setTxId("cb46a01a257194ecf7d6a1f7e1bee8ac4f0a6687ec13bb0bba8942377b64a6c4")
                                                .setIndex(0)
                                                .setBalance("102030")
                                                .build(),
                                            Utxo
                                                .newBuilder()
                                                .setTxId("c742a3e4257194ecf7d6a1f7e1bee8ac4b066a51ec13bb0bba8942377b64a6c4")
                                                .setIndex(1)
                                                .setBalance("123")
                                                .build(),
                                        ),
                                    ).build(),
                            ),
                        )
                }

            val act =
                reader
                    .readFromUpstream(up, Address.fromString(MainNetParams.get(), "38XnPvu9PmonFU9WouPXUjYbW91wa5MerL"))
                    .block(Duration.ofSeconds(1))

            act shouldHaveSize 2
            act[0].txid shouldBe "cb46a01a257194ecf7d6a1f7e1bee8ac4f0a6687ec13bb0bba8942377b64a6c4"
            act[0].vout shouldBe 0
            act[0].value shouldBe 102030
            act[1].txid shouldBe "c742a3e4257194ecf7d6a1f7e1bee8ac4b066a51ec13bb0bba8942377b64a6c4"
            act[1].vout shouldBe 1
            act[1].value shouldBe 123
        }
    })
