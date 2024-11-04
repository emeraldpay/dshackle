package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import org.apache.commons.collections4.functors.ConstantFactory
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class EthereumLocalReaderSpec extends Specification {

    def "Calls hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(
                new EthereumCachingReader(
                        TestingCommons.multistream(TestingCommons.api()),
                        Caches.default(),
                        ConstantFactory.constantFactory(new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)),
                        TestingCommons.tracerMock()
                ),
                methods,
                new EmptyHead(),
                null
        )
        when:
        def act = router.read(new ChainRequest("eth_coinbase", new ListParams())).block(Duration.ofSeconds(1))
        then:
        act.resultAsProcessedString == "0x0000000000000000000000000000000000000000"
    }

    def "Returns empty if nonce set"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(
                new EthereumCachingReader(
                        TestingCommons.multistream(TestingCommons.api()),
                        Caches.default(),
                        ConstantFactory.constantFactory(new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)),
                        TestingCommons.tracerMock()
                ),
                methods,
                new EmptyHead(),
                null
        )
        when:
        def act = router.read(new ChainRequest("eth_getTransactionByHash", new ListParams(["test"]), 10))
                .block(Duration.ofSeconds(1))
        then:
        act == null
    }

    def "getBlockByNumber with latest uses latest id"() {
        setup:
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 101L
        }
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            _ * txByHashAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            1 * blocksByHeightAsCont(Selector.UpstreamFilter.default) >> Mock(Reader) {
                1 * read(101L) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(101L), List.of())
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["latest", false], Selector.UpstreamFilter.default)

        then:
        act != null
        with(act.block()) {
            it.result.length > 0
            with(Global.objectMapper.readValue(it.result, BlockJson)) {
                number == 101
            }
        }
    }

    def "getBlockByNumber with earliest uses 0 block"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            _ * txByHashAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            1 * blocksByHeightAsCont(Selector.UpstreamFilter.default) >> Mock(Reader) {
                1 * read(0L) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(0L), List.of())
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["earliest", false], Selector.UpstreamFilter.default)

        then:
        act != null
        with(act.block()) {
            it.result.length > 0
            with(Global.objectMapper.readValue(it.result, BlockJson)) {
                number == 0
            }
        }
    }

    def "getBlockByNumber fetches the block"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            _ * txByHashAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            1 * blocksByHeightAsCont(Selector.UpstreamFilter.default) >> Mock(Reader) {
                1 * read(74735L) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(74735L), List.of())
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["0x123ef", false], Selector.UpstreamFilter.default)

        then:
        act != null
        with(act.block()) {
            it.result.length > 0
            with(Global.objectMapper.readValue(it.result, BlockJson)) {
                number == 74735
            }
        }
    }

    def "getBlockByNumber fetches the block by tag"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumCachingReader) {
            1 * blockByFinalization() >> Mock(Reader) {
                1 * read(FinalizationType.SAFE_BLOCK) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(74735L), List.of())
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.read(
                new ChainRequest("eth_getBlockByNumber",
                        new ListParams("safe", false))
        )

        then:
        act != null
        with(act.block()) {
            it.result.length > 0
            with(Global.objectMapper.readValue(it.result, BlockJson)) {
                number == 74735
            }
            it.finalization == new FinalizationData(74735, FinalizationType.SAFE_BLOCK)
        }
    }

    def "getBlockByNumber skips requests with tx bodies"() {
        setup:
        def head = Mock(Head)
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            _ * txByHashAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
            _ * blocksByHeightAsCont(Selector.UpstreamFilter.default) >> new EmptyReader<>()
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["0x0", true], Selector.UpstreamFilter.default)

        then:
        act == null
    }
}
