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
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.test.UpstreamsMock
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
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
            it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")
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
                                .setWeight(ByteString.copyFrom(blockJson.totalDifficulty.toByteArray()))
                        .setBlockId(blockJson.hash.toHex().substring(2))
                        .setTimestamp(blockJson.timestamp.toEpochMilli())
            ).build()

        def apiMock = TestingCommons.api(Stub(ReactorRpcClient))
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams, Schedulers.immediate())
        trackTx.init()

        apiMock.answer("eth_getTransactionByHash", [txId], txJson)
        apiMock.answer("eth_getBlockByHash", [blockJson.hash.toHex(), false], blockJson)
        upstreamMock.nextBlock(BlockContainer.from(blockHeadJson, TestingCommons.objectMapper()))

        when:
        def flux = trackTx.subscribe(req)
        then:
        StepVerifier.create(flux)
                .expectNext(exp1)
                .expectComplete()
                .verify(Duration.ofSeconds(4))
    }

    def "Closes for unknown transaction"() {
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

        def apiMock = TestingCommons.api(Stub(ReactorRpcClient))
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams, Schedulers.immediate())
        trackTx.init()

        apiMock.answer("eth_getTransactionByHash", [txId], null)

        when:
        def act = StepVerifier.withVirtualTime {
            return trackTx.subscribe(req)
        }
        then:
        act
                .expectNext(exp1)
                .then {
                    assert trackTx.notFound.any { it.tx.txid.toHex() == txId }
                }
                .expectNoEvent(Duration.ofSeconds(30))
                .then {
                    def track = trackTx.notFound.iterator().next()
                    track.tx
                            .copy(Instant.now() - Duration.ofHours(1), track, track.tx.status, Instant.now() - Duration.ofMinutes(15))
                            .makeCurrent()
                    trackTx.recheckNotFound()
                }
                .expectNext(exp1)
                .then {
                    assert !trackTx.notFound.any { it.tx.txid.toHex() == txId }
                }
                .expectComplete()
                .verify(Duration.ofSeconds(4))
    }

    def "Closes for known transaction if not mined"() {
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

        def apiMock = TestingCommons.api(Stub(ReactorRpcClient))
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams, Schedulers.immediate())
        trackTx.init()

        apiMock.answerOnce("eth_getTransactionByHash", [txId], null)
        apiMock.answer("eth_getTransactionByHash", [txId], txJson)

        when:
        def act = StepVerifier.withVirtualTime {
            return trackTx.subscribe(req)
        }
        then:
        act
                .expectNext(exp1)
                .then {
                    assert trackTx.notFound.any { it.tx.txid.toHex() == txId }
                }
                .expectNoEvent(Duration.ofSeconds(20))

                .then {
                    def track = trackTx.notFound.iterator().next()
                    track.tx
                            .copy(Instant.now() - Duration.ofSeconds(119), track, track.tx.status, Instant.now() - Duration.ofMinutes(15))
                            .makeCurrent()
                    trackTx.recheckNotFound()
                }
                .expectNext(exp2)
                .then {
                    assert trackTx.notFound.any { it.tx.txid.toHex() == txId }
                }
                .expectNoEvent(Duration.ofSeconds(20))

                .then {
                    def track = trackTx.notFound.iterator().next()
                    track.tx
                            .copy(Instant.now() - Duration.ofSeconds(121), track, track.tx.status, Instant.now() - Duration.ofMinutes(15))
                            .makeCurrent()
                    trackTx.recheckNotFound()
                }
                .expectNext(exp2)
                .expectComplete()
                .verify(Duration.ofSeconds(4))
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
                                .setWeight(ByteString.copyFrom(blocks[2].totalDifficulty.toByteArray()))
                                .setBlockId(blocks[2].hash.toHex().substring(2))
                                .setTimestamp(blocks[2].timestamp.toEpochMilli())
                )


        def apiMock = TestingCommons.api(Stub(ReactorRpcClient))
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams, Schedulers.immediate())
        trackTx.init()

        apiMock.answerOnce("eth_getTransactionByHash", [txId], null)
        apiMock.answerOnce("eth_getTransactionByHash", [txId], txJsonBroadcasted)
        apiMock.answer("eth_getTransactionByHash", [txId], txJsonMined)
        blocks.forEach { block ->
            apiMock.answer("eth_getBlockByHash", [block.hash.toHex(), false], block)
        }

        def nextBlock = { int i ->
            return {
                println("block $i");
                upstreamMock.nextBlock(BlockContainer.from(blocks[i], TestingCommons.objectMapper()))
            } as Runnable
        }

        when:
        def flux = trackTx.subscribe(req)
        then:
        StepVerifier.create(flux)
                .expectNext(exp1.build())
                .then(nextBlock(1))
                .expectNext(exp1.setBroadcasted(true).build())
                .then(nextBlock(2))
                .expectNext(exp2.setConfirmations(1).build())
                .then(nextBlock(3))
                .expectNext(exp2.setConfirmations(2).build())
                .then(nextBlock(4))
                .expectNext(exp2.setConfirmations(3).build())
                .then(nextBlock(5))
                .expectNext(exp2.setConfirmations(4).build())
                .expectComplete()
                .verify(Duration.ofSeconds(4))
    }

    def "Tracked after first load"() {
        setup:
        def apiMock = TestingCommons.api(Stub(ReactorRpcClient))
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams, Schedulers.immediate())
        trackTx.init()

        def req = BlockchainOuterClass.TxStatusRequest.newBuilder()
                .setChain(chain)
                .setConfirmationLimit(6)
                .setTxId(txId)
                .build()
        def tx = trackTx.prepareTracking(req)
        when:
        trackTx.onFirstUpdate(tx)
        then:
        trackTx.notFound.any { it.tx.txid == TransactionId.from(txId) }
        trackTx.trackedForChain(Chain.ETHEREUM).any { it.txid == TransactionId.from(txId) }
    }

    def "Update of last notified keeps everything else"() {
        setup:
        def apiMock = TestingCommons.api(Stub(ReactorRpcClient))
        def upstreamMock = TestingCommons.upstream(apiMock)
        Upstreams upstreams = new UpstreamsMock(Chain.ETHEREUM, upstreamMock)
        TrackEthereumTx trackTx = new TrackEthereumTx(upstreams, Schedulers.immediate())
        trackTx.init()

        def req = BlockchainOuterClass.TxStatusRequest.newBuilder()
                .setChain(chain)
                .setConfirmationLimit(6)
                .setTxId(txId)
                .build()
        def tx = trackTx.prepareTracking(req)
        when:
        tx = tx.withStatus(true, 100,
                true, BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22"), Instant.now(),
                1000 as BigInteger, 15L)
        then:
        tx.status.found
        tx.notifiedAt.isBefore(Instant.now() - Duration.ofSeconds(5))
        when:
        def c = tx.justNotified()
        then:
        tx.notifiedAt.isBefore(Instant.now() - Duration.ofSeconds(5))
        c.notifiedAt.isAfter(Instant.now() - Duration.ofSeconds(5))
        c.status.found
        c.status.height == 100
        c.status.mined
        c.status.blockHash.toHex() == "0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22"
        c.status.blockTime.isAfter(Instant.now() - Duration.ofSeconds(5))
        c.status.blockTotalDifficulty == 1000 as BigInteger
        c.status.confirmations == 15L
    }
}
