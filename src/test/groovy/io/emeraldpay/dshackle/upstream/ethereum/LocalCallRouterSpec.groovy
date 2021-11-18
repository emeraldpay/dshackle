package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.reader.EmptyReader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.rpc.json.BlockJson
import org.apache.commons.collections4.functors.ConstantFactory
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class LocalCallRouterSpec extends Specification {

    def "Calls hardcoded"() {
        setup:
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        def router = new LocalCallRouter(
                new EthereumReader(
                        TestingCommons.multistream(TestingCommons.api()),
                        Caches.default(),
                        ConstantFactory.constantFactory(new DefaultEthereumMethods(Chain.ETHEREUM))
                ),
                methods,
                new EmptyHead()
        )
        when:
        def act = router.read(new JsonRpcRequest("eth_coinbase", [])).block(Duration.ofSeconds(1))
        then:
        act.resultAsProcessedString == "0x0000000000000000000000000000000000000000"
    }

    def "getBlockByNumber with latest uses latest id"() {
        setup:
        def head = Mock(Head) {
            1 * getCurrentHeight() >> 101L
        }
        def reader = Mock(EthereumReader) {
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            1 * blocksByHeightAsCont() >> Mock(Reader) {
                1 * read(101L) >> Mono.just(TestingCommons.blockForEthereum(101L))
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        def router = new LocalCallRouter(reader, methods, head)

        when:
        def act = router.getBlockByNumber(["latest", false])

        then:
        act != null
        with(act.block()) {
            it.length > 0
            with(Global.objectMapper.readValue(it, BlockJson)) {
                number == 101
            }
        }
    }

    def "getBlockByNumber with earliest uses 0 block"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumReader) {
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            1 * blocksByHeightAsCont() >> Mock(Reader) {
                1 * read(0L) >> Mono.just(TestingCommons.blockForEthereum(0L))
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        def router = new LocalCallRouter(reader, methods, head)

        when:
        def act = router.getBlockByNumber(["earliest", false])

        then:
        act != null
        with(act.block()) {
            it.length > 0
            with(Global.objectMapper.readValue(it, BlockJson)) {
                number == 0
            }
        }
    }

    def "getBlockByNumber fetches the block"() {
        setup:
        def head = Stub(Head) {}
        def reader = Mock(EthereumReader) {
            _ * blocksByIdAsCont() >> new EmptyReader<>()
            _ * txByHashAsCont() >> new EmptyReader<>()
            1 * blocksByHeightAsCont() >> Mock(Reader) {
                1 * read(74735L) >> Mono.just(TestingCommons.blockForEthereum(74735L))
            }
        }
        def methods = new DefaultEthereumMethods(Chain.ETHEREUM)
        def router = new LocalCallRouter(reader, methods, head)

        when:
        def act = router.getBlockByNumber(["0x123ef", false])

        then:
        act != null
        with(act.block()) {
            it.length > 0
            with(Global.objectMapper.readValue(it, BlockJson)) {
                number == 74735
            }
        }
    }
}
