/**
 * Copyright (c) 2019 ETCDEV GmbH
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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.test.UpstreamsMock
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.ethereum.EthereumReader
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.RpcCall
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class TrackEthereumAddressSpec extends Specification {

    def chain = Common.ChainRef.CHAIN_ETHEREUM
    def address1 = "0xe2c8fa8120d813cd0b5e6add120295bf20cfa09f"
    def address1Proto = Common.SingleAddress.newBuilder()
            .setAddress(address1)
    def etherAsset = Common.Asset.newBuilder()
            .setChain(chain)
            .setCode("ETHER")


    def "get balance"() {
        setup:
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAsset(etherAsset)
                .setAddress(Common.AnyAddress.newBuilder().setAddressSingle(address1Proto).build())
                .build()
        def exp = BlockchainOuterClass.AddressBalance.newBuilder()
            .setAddress(address1Proto)
            .setAsset(etherAsset)
            .setBalance("1234567890")
            .build()

        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumAddress trackAddress = new TrackEthereumAddress(upstreams)

        apiMock.answer("eth_getBalance", ["0xe2c8fa8120d813cd0b5e6add120295bf20cfa09f", "latest"], "0x499602D2")
        when:
        def flux = trackAddress.getBalance(req)
        then:
        StepVerifier.create(flux)
            .expectNext(exp)
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    def "recheck address after each block"() {
        setup:
        def req = BlockchainOuterClass.BalanceRequest.newBuilder()
                .setAsset(etherAsset)
                .setAddress(Common.AnyAddress.newBuilder().setAddressSingle(address1Proto).build())
                .build()
        def exp1 = BlockchainOuterClass.AddressBalance.newBuilder()
                .setAddress(address1Proto)
                .setAsset(etherAsset)
                .setBalance("1234567890")
                .build()
        def exp2 = BlockchainOuterClass.AddressBalance.newBuilder()
                .setAddress(address1Proto)
                .setAsset(etherAsset)
                .setBalance("65432")
                .build()

        def block2 = new BlockJson().with {
            it.number = 1
            it.totalDifficulty = 100
            it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")
            it.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
            return it
        }

        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumAddress trackAddress = new TrackEthereumAddress(upstreams)

        apiMock.answerOnce("eth_getBalance", ["0xe2c8fa8120d813cd0b5e6add120295bf20cfa09f", "latest"], "0x499602D2")
        apiMock.answerOnce("eth_getBalance", ["0xe2c8fa8120d813cd0b5e6add120295bf20cfa09f", "latest"], "0xff98")
        when:
        def flux = trackAddress.subscribe(req)
        then:
        StepVerifier.create(flux)
                .expectNext(exp1).as("First block")
                .then {
                    upstreamMock.nextBlock(BlockContainer.from(block2, TestingCommons.objectMapper()))
                    Thread.sleep(50)
                }
                .expectNext(exp2).as("Second block")
                .thenCancel()
                .verify(Duration.ofSeconds(1))
    }
}
