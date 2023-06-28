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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.test.EthereumPosRpcUpstreamMock
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ethereum.EthereumPosRpcUpstream
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class StreamHeadSpec extends Specification {

    ObjectMapper objectMapper = Global.objectMapper

    def "Errors on unavailable chain"() {
        setup:
        def upstreams = new MultistreamHolderMock(Chain.ETHEREUM__MAINNET, Stub(EthereumPosRpcUpstream))
        def streamHead = new StreamHead(upstreams)
        when:
        def flux = streamHead.add(
                Mono.just(Common.Chain.newBuilder().setType(Common.ChainRef.CHAIN_ETHEREUM_CLASSIC__MAINNET).build())
        )
        then:
        StepVerifier.create(flux)
                .expectError()
                .verify(Duration.ofSeconds(1))
    }

    def "Subscribes through upstream head"() {
        setup:

        def blocks = (100..105).collect { i ->
            return new BlockJson<TransactionRefJson>().with {
                it.number = i
                it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27${i}")
                it.parentHash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27${i}")
                it.totalDifficulty = i * 1000
                it.timestamp = Instant.ofEpochMilli(1566000000000 + i * 10000)
                return it
            }
        }

        def heads = blocks.collect {
            return BlockchainOuterClass.ChainHead.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                .setTimestamp(it.timestamp.toEpochMilli())
                .setBlockId(it.hash.toHex().substring(2))
                .setWeight(ByteString.copyFrom(it.totalDifficulty.toByteArray()))
                .setParentBlockId(it.parentHash.toHex().substring(2))
                .setHeight(it.number)
                .build()
        }

        def upstream = new EthereumPosRpcUpstreamMock(Chain.ETHEREUM__MAINNET, TestingCommons.api())
        def upstreams = new MultistreamHolderMock(Chain.ETHEREUM__MAINNET, upstream)
        def streamHead = new StreamHead(upstreams)
        when:
        def flux = streamHead.add(
                Mono.just(Common.Chain.newBuilder().setType(Common.ChainRef.CHAIN_ETHEREUM__MAINNET).build())
        )
        then:
        StepVerifier.create(flux.take(2))
                .then {
                    upstream.nextBlock(BlockContainer.from(blocks[0]))
                }
                .expectNext(heads[0])
                .then { upstream.nextBlock(BlockContainer.from(blocks[1])) }
                .expectNext(heads[1])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
