package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.UpstreamsMock
import io.emeraldpay.dshackle.upstream.EthereumApi
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

class StreamHeadSpec extends Specification {

    def "Errors on unavailable chain"() {
        setup:
        def upstreams = new UpstreamsMock(Chain.ETHEREUM, Stub(Upstream))
        def streamHead = new StreamHead(upstreams)
        when:
        def flux = streamHead.add(
                Mono.just(Common.Chain.newBuilder().setType(Common.ChainRef.CHAIN_ETHEREUM_CLASSIC).build())
        )
        then:
        StepVerifier.create(flux)
                .expectError()
                .verify(Duration.ofSeconds(1))
    }

    def "Subscribes through upstream head"() {
        setup:

        def blocks = (100..105).collect { i ->
            return new BlockJson<TransactionId>().with {
                it.number = i
                it.hash = BlockHash.from("0xa0e65cbc1b52a8ca60562112c6060552d882f16f34a9dba2ccdc05c0a6a27${i}")
                it.totalDifficulty = i * 1000
                it.timestamp = new Date(1566000000000 + i * 10000)
                return it
            }
        }

        def heads = blocks.collect {
            return BlockchainOuterClass.ChainHead.newBuilder()
                .setChain(Common.ChainRef.CHAIN_ETHEREUM)
                .setTimestamp(it.timestamp.time)
                .setBlockId(it.hash.toHex().substring(2))
                .setWeight(ByteString.copyFrom(it.totalDifficulty.toByteArray()))
                .setHeight(it.number)
                .build()
        }

        def upstream = new EthereumUpstreamMock(Chain.ETHEREUM, Mock(EthereumApi))
        def upstreams = new UpstreamsMock(Chain.ETHEREUM, upstream)
        def streamHead = new StreamHead(upstreams)
        when:
        def flux = streamHead.add(
                Mono.just(Common.Chain.newBuilder().setType(Common.ChainRef.CHAIN_ETHEREUM).build())
        )
        then:
        StepVerifier.create(flux.take(2))
                .then { upstream.nextBlock(blocks[0]) }
                .expectNext(heads[0])
                .then { upstream.nextBlock(blocks[1]) }
                .expectNext(heads[1])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
