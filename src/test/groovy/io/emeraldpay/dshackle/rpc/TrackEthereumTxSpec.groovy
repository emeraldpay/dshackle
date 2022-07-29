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

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.test.MultistreamHolderMock
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import reactor.test.scheduler.VirtualTimeScheduler
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class TrackEthereumTxSpec extends Specification {

    def chain = Common.ChainRef.CHAIN_ETHEREUM
    def txId = "0xba61ce4672751fd6086a9ac2b55547a5555af17535b6c0334ede2ecb6d64070a"


    def "Gives details for an old transaction"() {
        setup:
        def req = BlockchainOuterClass.TxStatusRequest.newBuilder()
                .setChain(chain)
                .setConfirmationLimit(6)
            .setTxId(txId)
            .build()

        def blockJson = new BlockJson().with {
            it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")
            it.timestamp = Instant.ofEpochMilli(156400000000)
            it.number = 100
            it.totalDifficulty = BigInteger.valueOf(500)
            it
        }

        def blockHeadJson = new BlockJson().with {
            it.hash = BlockHash.from("0x552d882f16f34a9dba2ccdc05c0a6a27c22a0e65cbc1b52a8ca60562112c6060")
            it.timestamp = Instant.ofEpochMilli(156400200000)
            it.number = 108
            it.totalDifficulty = BigInteger.valueOf(800)
            it.transactions = []
            it
        }


        def txJson = new TransactionJson().with {
            it.hash = TransactionId.from("0xba61ce4672751fd6086a9ac2b55547a5555af17535b6c0334ede2ecb6d64070a")
            it.blockHash = blockJson.hash
            it.blockNumber = blockJson.number
            it.nonce = 1
            it
        }

        blockJson.transactions = [new TransactionRefJson(txJson.hash)]

        def exp1 = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(txId)
                .setBroadcasted(true)
                .setMined(true)
                .setConfirmations(8 + 1)
                .setBlock(
                        Common.BlockInfo.newBuilder()
                                .setHeight(blockJson.number)
                        .setBlockId(blockJson.hash.toHex().substring(2))
                        .setTimestamp(blockJson.timestamp.toEpochMilli())
            ).build()

        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams)

        apiMock.answer("eth_getTransactionByHash", [txId], txJson)
        apiMock.answer("eth_getBlockByHash", [blockJson.hash.toHex(), false], blockJson)
        upstreamMock.nextBlock(BlockContainer.from(blockHeadJson))

        when:
        def flux = trackTx.subscribe(req)
        then:
        StepVerifier.create(flux)
                .expectNext(exp1)
                .expectComplete()
                .verify(Duration.ofSeconds(4))
    }

    def "Wait for unknown transaction"() {
        setup:
        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.ETHEREUM, upstreamMock)
        ((EthereumMultistream) upstreams.getUpstream(Chain.ETHEREUM)).head = Mock(Head) {
            _ * getFlux() >> Flux.empty()
        }
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams)
        def scheduler = VirtualTimeScheduler.create(true)
        trackTx.scheduler = scheduler

        apiMock.answer("eth_getTransactionByHash", [txId], null)

        when:
        def tx = new TrackEthereumTx.TxDetails(Chain.ETHEREUM, Instant.now(), TransactionId.from(txId), 6)
        def act = StepVerifier.withVirtualTime(
                { trackTx.subscribe(tx, upstreams.getUpstream(Chain.ETHEREUM).cast(EthereumMultistream)) },
                { scheduler },
                5)

        then:
        act
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(20)).as("Waited for updates")
                .expectComplete()
                .verify(Duration.ofSeconds(3))
    }

    def "Known transaction when not mined"() {
        setup:
        def req = BlockchainOuterClass.TxStatusRequest.newBuilder()
                .setChain(chain)
                .setConfirmationLimit(6)
                .setTxId(txId)
                .build()
        def exp1 = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(txId)
                .setBroadcasted(false)
                .setMined(false)
                .build()
        def exp2 = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(txId)
                .setBroadcasted(true)
                .setMined(false)
                .build()
        def txJson = new TransactionJson().with {
            it.hash = TransactionId.from(txId)
            it.nonce = 1
            it
        }

        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams)
        def scheduler = VirtualTimeScheduler.create(true)
        trackTx.scheduler = scheduler

        apiMock.answerOnce("eth_getTransactionByHash", [txId], null)
        apiMock.answer("eth_getTransactionByHash", [txId], txJson)

        when:
        def act = StepVerifier.withVirtualTime({
            return trackTx.subscribe(req).take(2)
        }, { scheduler }, 5)
        then:
        act
                .expectSubscription()
                .expectNext(exp1).as("Unknown tx")
                .expectNext(exp2).as("Found in mempool")
                .expectComplete()
                .verify(Duration.ofSeconds(4))
    }

    def "New block makes tx mined"() {
        setup:
        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams)

        def tx = new TrackEthereumTx.TxDetails(Chain.ETHEREUM, Instant.now(), TransactionId.from(txId), 6)
        def block = new BlockContainer(
                100, BlockId.from(txId), BigInteger.ONE, Instant.now(), false, "".bytes, null,
                [TxId.from(txId)], 0
        )

        when:
        def act = trackTx.onNewBlock(tx, block)
        then:
        StepVerifier.create(act)
                .expectNext(tx.withStatus(true, 100, true, BlockHash.from(txId), block.timestamp, BigInteger.ONE, 1))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "New block without current tx requires a call"() {
        setup:
        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams)

        def tx = new TrackEthereumTx.TxDetails(Chain.ETHEREUM, Instant.now(), TransactionId.from(txId), 6)
        def block = new BlockContainer(
                100, BlockId.from(txId), BigInteger.ONE, Instant.now(), false, "".bytes, null,
                [TxId.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")],
                0
        )
        apiMock.answer("eth_getTransactionByHash", [txId], null)

        when:
        def act = trackTx.onNewBlock(tx, block)
        then:
        StepVerifier.create(act)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Starts to follow new transaction"() {
        setup:
        def req = BlockchainOuterClass.TxStatusRequest.newBuilder()
                .setChain(chain)
                .setConfirmationLimit(4)
                .setTxId(txId)
                .build()

        List<BlockJson<TransactionRefJson>> blocks = (0..9).collect { i ->
            return new BlockJson().with {
                it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a2000${i}")
                it.timestamp = Instant.ofEpochMilli(156400000000 + i * 10000)
                it.setNumber(100L + i.longValue())
                it.totalDifficulty = BigInteger.valueOf(500 + i)
                it
            }
        }

        def txJsonBroadcasted = new TransactionJson().with {
            it.hash = TransactionId.from("0xba61ce4672751fd6086a9ac2b55547a5555af17535b6c0334ede2ecb6d64070a")
            it.blockHash = null
            it.blockNumber = null
            it.nonce = 1
            it
        }

        def txJsonMined = new TransactionJson().with {
            it.hash = TransactionId.from("0xba61ce4672751fd6086a9ac2b55547a5555af17535b6c0334ede2ecb6d64070a")
            it.blockHash = blocks[2].hash
            it.blockNumber = blocks[2].number
            it.nonce = 1
            it
        }

        def exp1 = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(txId)
                .setBroadcasted(false)
                .setMined(false)
                .setConfirmations(0)

        def exp2 = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(txId)
                .setBroadcasted(true)
                .setMined(true)
                .setBlock(
                        Common.BlockInfo.newBuilder()
                                .setHeight(blocks[2].number)
                                .setBlockId(blocks[2].hash.toHex().substring(2))
                                .setTimestamp(blocks[2].timestamp.toEpochMilli())
                )


        def apiMock = TestingCommons.api()
        def upstreamMock = TestingCommons.upstream(apiMock)
        MultistreamHolder upstreams = new MultistreamHolderMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams)

        apiMock.answerOnce("eth_getTransactionByHash", [txId], null)
        apiMock.answerOnce("eth_getTransactionByHash", [txId], txJsonBroadcasted)
        apiMock.answerOnce("eth_getTransactionByHash", [txId], txJsonBroadcasted)
        apiMock.answer("eth_getTransactionByHash", [txId], txJsonMined)
        blocks.forEach { block ->
            apiMock.answer("eth_getBlockByHash", [block.hash.toHex(), false], block)
        }

        upstreamMock.blocks = Flux.fromIterable(blocks)
                .map { block ->
                    BlockContainer.from(block)
                }

        when:
        def flux = trackTx.subscribe(req)
        then:
        StepVerifier.create(flux)
                .expectNext(exp1.build()).as("Just empty")
                .expectNext(exp1.setBroadcasted(true).build()).as("Found in mempool")
                .expectNext(exp2.setConfirmations(1).build()).as("Mined")
                .expectNext(exp2.setConfirmations(2).build()).as("Confirmed 2")
                .expectNext(exp2.setConfirmations(3).build()).as("Confirmed 3")
                .expectNext(exp2.setConfirmations(4).build()).as("Confirmed 4")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

}
