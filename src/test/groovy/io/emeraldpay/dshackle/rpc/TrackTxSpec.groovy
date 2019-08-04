package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.test.EthereumApiMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.AggregatedUpstreams
import io.emeraldpay.dshackle.upstream.AvailableChains
import io.emeraldpay.dshackle.upstream.EthereumHead
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.RpcClient
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class TrackTxSpec extends Specification {

    AvailableChains availableChains = new AvailableChains(TestingCommons.objectMapper())
    Upstreams upstreams
    TrackTx trackTx

    def chain = Common.ChainRef.CHAIN_ETHEREUM
    def txId = "0xba61ce4672751fd6086a9ac2b55547a5555af17535b6c0334ede2ecb6d64070a"

    def setup() {
        upstreams = Mock(Upstreams)
        trackTx = new TrackTx(upstreams, availableChains, Schedulers.immediate())
    }

    def start() {
        trackTx.init()
        availableChains.add(Chain.ETHEREUM)
        availableChains.add(Chain.TESTNET_KOVAN)
    }

    def "Gives details for an old transaction"() {
        setup:
        def req = BlockchainOuterClass.TxStatusRequest.newBuilder()
            .setChain(chain)
            .setConfirmationLimit(6)
            .setTxId(txId)
            .build()

        def blockJson = new BlockJson().with {
            it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")
            it.timestamp = new Date(156400000000)
            it.number = 100
            it.totalDifficulty = BigInteger.valueOf(500)
            it
        }

        def blockHeadJson = new BlockJson().with {
            it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27c22")
            it.timestamp = new Date(156400200000)
            it.number = 108
            it.totalDifficulty = BigInteger.valueOf(800)
            it
        }


        def txJson = new TransactionJson().with {
            it.hash = TransactionId.from("0xba61ce4672751fd6086a9ac2b55547a5555af17535b6c0334ede2ecb6d64070a")
            it.blockHash = blockJson.hash
            it.blockNumber = blockJson.number
            it.nonce = 1
            it
        }

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
                        .setTimestamp(blockJson.timestamp.getTime())
            ).build()


        def upstreamMock = Mock(AggregatedUpstreams)
        def blocksBus = TopicProcessor.create()
        def headMock = Mock(EthereumHead)

        def apiMock = TestingCommons.api(Stub(RpcClient), upstreamMock)
        apiMock.answer("eth_getTransactionByHash", [txId], txJson)
        apiMock.answer("eth_getBlockByHash", [blockJson.hash.toHex(), false], blockJson)

        _ * upstreams.getUpstream(Chain.ETHEREUM) >> upstreamMock
        _ * upstreamMock.getApi(_) >> apiMock
        _ * upstreamMock.getHead() >> headMock
        _ * headMock.getFlux() >> blocksBus
        _ * headMock.getHead() >> Mono.just(blockHeadJson)
        start()

        when:
        def flux = trackTx.add(Mono.just(req))
        then:
        StepVerifier.create(flux)
                .expectNext(exp1)
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

        List<BlockJson<TransactionId>> blocks = (0..9).collect { i ->
            return new BlockJson().with {
                it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a2000${i}")
                it.timestamp = new Date(156400000000 + i * 10000)
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
                                .setTimestamp(blocks[2].timestamp.getTime())
                )


        def upstreamMock = Mock(AggregatedUpstreams)
        def blocksBus = TopicProcessor.create()
        def headMock = Mock(EthereumHead)

        def apiMock = TestingCommons.api(Stub(RpcClient), upstreamMock)
        apiMock.answerOnce("eth_getTransactionByHash", [txId], null)
        apiMock.answerOnce("eth_getTransactionByHash", [txId], txJsonBroadcasted)
        apiMock.answer("eth_getTransactionByHash", [txId], txJsonMined)
        blocks.forEach { block ->
            apiMock.answer("eth_getBlockByHash", [block.hash.toHex(), false], block)
        }

        def headBlock = blocks[0]

        _ * upstreams.getUpstream(Chain.ETHEREUM) >> upstreamMock
        _ * upstreamMock.getApi(_) >> apiMock
        _ * upstreamMock.getHead() >> headMock
        _ * headMock.getFlux() >> blocksBus
        _ * headMock.getHead() >> { return Mono.just(headBlock) }
        start()

        def nextBlock = { int i ->
            return {
                println("block $i");
                headBlock = blocks[i];
                blocksBus.onNext(blocks[i])
            } as Runnable
        }

        when:
        def flux = trackTx.add(Mono.just(req))
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
}
