package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.HeightCache
import io.emeraldpay.dshackle.test.TestingCommons
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.rpc.ReactorRpcClient
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import io.infinitape.etherjar.rpc.ws.WebsocketClient
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

class EthereumWsSpec extends Specification {

    def "Uses cache to fetch block"() {
        setup:
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        def ws = new EthereumWs(new URI("http://localhost"), new URI("http://localhost"), apiMock)
        def blocksCache = Mock(BlocksMemCache)
        def caches = Caches.newBuilder().setBlockByHash(blocksCache).build()
        ws.setCaches(caches)

        def block = new BlockJson<TransactionRefJson>()
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)

        when:
        ws.onNewBlock(block)

        then:
        1 * blocksCache.read(BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")) >> Mono.just(block)
        StepVerifier.create(ws.flux.take(1))
            .expectNext(block)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    def "Fetch block if cache is empty"() {
        setup:
        ReactorRpcClient rpcClient = Stub(ReactorRpcClient)
        def apiMock = TestingCommons.api(rpcClient)
        def ws = new EthereumWs(new URI("http://localhost"), new URI("http://localhost"), apiMock)
        def blocksCache = Mock(BlocksMemCache)
        def caches = Caches.newBuilder().setBlockByHash(blocksCache).build()
        ws.setCaches(caches)

        def block = new BlockJson<TransactionRefJson>()
        block.hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200")
        block.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
        block.transactions = []
        block.uncles = []

        apiMock.answerOnce("eth_getBlockByHash", ["0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8915200", false], block)

        when:
        ws.onNewBlock(block)

        then:
        1 * blocksCache.read(_) >> Mono.empty()
        StepVerifier.create(ws.flux.take(1))
                .expectNext(block)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
}
