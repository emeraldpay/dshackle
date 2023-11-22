package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
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
        def act = router.read(new JsonRpcRequest("eth_coinbase", [])).block(Duration.ofSeconds(1))
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
        def act = router.read(new JsonRpcRequest("eth_getTransactionByHash", ["test"], 10))
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
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            1 * blocksByHeightAsCont() >> Mock(Reader) {
                1 * read(101L) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(101L), null)
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["latest", false])

        then:
        act != null
        with(act.block()) {
            it.first.length > 0
            with(Global.objectMapper.readValue(it.first, BlockJson)) {
                number == 101
            }
        }
    }

    def "getBlockByNumber with earliest uses 0 block"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            1 * blocksByHeightAsCont() >> Mock(Reader) {
                1 * read(0L) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(0L), null)
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["earliest", false])

        then:
        act != null
        with(act.block()) {
            it.first.length > 0
            with(Global.objectMapper.readValue(it.first, BlockJson)) {
                number == 0
            }
        }
    }

    def "getBlockByNumber fetches the block"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            1 * blocksByHeightAsCont() >> Mock(Reader) {
                1 * read(74735L) >> Mono.just(
                        new EthereumDirectReader.Result<>(TestingCommons.blockForEthereum(74735L), null)
                )
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["0x123ef", false])

        then:
        act != null
        with(act.block()) {
            it.first.length > 0
            with(Global.objectMapper.readValue(it.first, BlockJson)) {
                number == 74735
            }
        }
    }

    def "getBlockByNumber skips requests with tx bodies"() {
        setup:
        def head = Mock(Head)
        def reader = Mock(EthereumCachingReader) {
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            _ * blocksByHeightAsCont() >> new EmptyReader<>()
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        def router = new EthereumLocalReader(reader, methods, head, null)

        when:
        def act = router.getBlockByNumber(["0x0", true])

        then:
        act == null
    }
}
