package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.ApiReaderMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.BlockHash
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import spock.lang.Specification
import java.time.Duration

class EnrichedMergedHeadSpec extends Specification {

    def "ensures that heads are running on start"() {
        setup:
        def head1 = Mock(TestHead) {
            _ * isRunning() >> false
            _ * getFlux() >> Flux.empty()
        }
        def head2 = Mock(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.empty()
        }
        def head3 = Mock(TestHead) {
            _ * isRunning() >> false
            _ * getFlux() >> Flux.empty()
        }

        def api = new ApiReaderMock()
        when:
        def merged = new EnrichedMergedHead([head1, head2], head3, Schedulers.parallel(), new BlockReader(api))
        merged.start()

        then:
        1 * head3.start()
        1 * head1.start()
        merged.isRunning()
    }

    def "if enriched block comes from reference head we pass it along instantly"() {
        setup:
        def block = TestingCommons.enrichedBlockForEthereum(100)
        def api = new ApiReaderMock()
        def head = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.just(block)
        }
        when:
        def merge = new EnrichedMergedHead([], head, Schedulers.parallel(), new BlockReader(api))

        then:
        StepVerifier.create(merge.getFlux())
            .then { merge.start() }
            .expectNext(block)
            .thenCancel()
            .verify(Duration.ofMillis(100))
        block.enriched
    }

    def "enriched block arrived in sources before reference block"() {
        setup:
        def enrichedBlock = TestingCommons.enrichedBlockForEthereum(100)
        def block = TestingCommons.blockForEthereum(100)
        Sinks.Many<BlockContainer> refSink = Sinks.many().multicast().directBestEffort()
        def headRef = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> refSink.asFlux()
        }
        def headSource = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.just(enrichedBlock)
        }
        when:
        def merge = new EnrichedMergedHead([headSource], headRef, Schedulers.parallel(), new BlockReader(new ApiReaderMock()))
        then:
        StepVerifier.create(merge.getFlux())
            .then { merge.start() }
            .expectNoEvent(Duration.ofMillis(100))
            .then { refSink.tryEmitNext(block) }
            .expectNext(enrichedBlock)
            .thenCancel()
            .verify(Duration.ofMillis(200))
    }

    def "enriched block arrived in source after reference block, but before deadline"() {
        setup:
        def enrichedBlock = TestingCommons.enrichedBlockForEthereum(100)
        def block = TestingCommons.blockForEthereum(100)
        Sinks.Many<BlockContainer> sourceSink = Sinks.many().multicast().directBestEffort()
        def headRef = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.just(block)
        }
        def headSource = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> sourceSink.asFlux()
        }
        when:
        def merge = new EnrichedMergedHead([headSource], headRef, Schedulers.parallel(), new BlockReader(new ApiReaderMock()))
        then:
        StepVerifier.create(merge.getFlux())
            .then { merge.start() }
            .expectNoEvent(Duration.ofMillis(600))
            .then { sourceSink.tryEmitNext(enrichedBlock) }
            .expectNext(enrichedBlock)
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    def "enriched blocks does not arrive before deadline"() {
        setup:
        def enrichedBlock = TestingCommons.enrichedBlockForEthereum(100)
        def block = TestingCommons.blockForEthereum(100)
        def headRef = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.just(block)
        }
        def headSource = Stub(TestHead) {
            _ * isRunning() >> true
            _ * getFlux() >> Flux.just(block)
        }
        def api = new ApiReaderMock().tap {
            answer("eth_getBlockByHash", [block.hash.toHexWithPrefix(), false], enrichedBlock.toBlock())
        }
        when:
        def merge = new EnrichedMergedHead([headSource], headRef, Schedulers.parallel(), new BlockReader(api))
        then:
        StepVerifier.create(merge.getFlux())
            .then { merge.start() }
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext(enrichedBlock)
            .thenCancel()
            .verify(Duration.ofMillis(1200))
    }

    class BlockReader implements Reader<BlockHash, BlockContainer> {
        private ApiReaderMock mockApi
        BlockReader(ApiReaderMock api) {
            mockApi = api
        }

        Mono<BlockContainer> read(BlockHash hash) {
            return mockApi.read(new JsonRpcRequest("eth_getBlockByHash", [hash.toHex(), false]))
                    .map {
                        def t = it
                        def a = 1
                        return it
                    }
                    .flatMap(JsonRpcResponse::requireResult)
                    .map { BlockContainer.fromEthereumJson(it, "test") }
        }
    }

    class TestHead extends AbstractHead implements Lifecycle {

        TestHead() {
            super(new MostWorkForkChoice(), Schedulers.parallel(), new BlockValidator.AlwaysValid(), 100_000)
        }

        @Override
        void start() {

        }

        @Override
        void stop() {

        }

        @Override
        boolean isRunning() {
            return false
        }
    }
}
