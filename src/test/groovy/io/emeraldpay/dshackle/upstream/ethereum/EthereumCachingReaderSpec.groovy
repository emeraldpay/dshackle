package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.quorum.QuorumReader
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import org.apache.commons.collections4.Factory
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class EthereumDirectReaderSpec extends Specification {

    String hash1 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String address1 = "0xe0aadb0a012dbcdc529c4c743d3e0385a0b54d3d"
    Upstream resolver = TestingCommons.upstream()

    def "Reads block by hash"() {
        setup:
        def json = new BlockJson().tap {
            number = 100
            hash = BlockHash.from(hash1)
            timestamp = Instant.now()
            totalDifficulty = BigInteger.ONE
            parentHash = BlockHash.from(hash1)
            transactions = []
        }
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _,) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBlockByHash", [hash1, false])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver)
                )
            }
        }
        when:
        def act = reader.blockReader.read(BlockHash.from(hash1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.data.hash.toHexWithPrefix() == hash1
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Produce empty result on non-existing block"() {
        setup:
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBlockByHash", [hash1, false])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(null), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.blockReader.read(BlockHash.from(hash1))
        then:
        StepVerifier.create(act)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads block by height"() {
        setup:
        def json = new BlockJson().tap {
            number = 100
            hash = BlockHash.from(hash1)
            timestamp = Instant.now()
            totalDifficulty = BigInteger.ONE
            parentHash = BlockHash.from(hash1)
            transactions = []
        }
        def up = Mock(Multistream) {
            1 * getApiSource(
                    new Selector.Builder().withMatcher(new Selector.HeightMatcher(100)).forMethod("eth_getBlockByNumber").build()
            ) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x64", false])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.blockByHeightReader.read(100)
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.data.hash.toHexWithPrefix() == hash1
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads tx"() {
        setup:
        def json = new TransactionJson().tap {
            hash = TransactionId.from(hash1)
            blockNumber = 100
            blockHash = BlockHash.from(hash1)
        }
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getTransactionByHash", [hash1])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.txReader.read(TransactionId.from(hash1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.data.hash.toHexWithPrefix() == hash1
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads tx receipt"() {
        setup:
        def json = new TransactionReceiptJson().tap {
            transactionHash = TransactionId.from(hash1)
            blockNumber = 100
            blockHash = BlockHash.from(hash1)
        }
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getTransactionReceipt", [hash1])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.receiptReader.read(TransactionId.from(hash1))
            .block(Duration.ofSeconds(1))
            .with { new String(it.data) }
        then:
        act == '{"blockHash":"0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5","blockNumber":"0x64","transactionHash":"0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5","logs":[]}'
    }

    def "Puts tx receipt in cache after reading"() {
        setup:
        def json = new TransactionReceiptJson().tap {
            transactionHash = TransactionId.from(hash1)
            blockNumber = 100
            blockHash = BlockHash.from(hash1)
        }
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        def caches = Mock(Caches) {
            // note that the Caches needs a Height value, otherwise it's not cached
            1 * cacheReceipt(Caches.Tag.REQUESTED, { DefaultContainer data -> data.txId.toHex() == hash1.substring(2) && data.height == 100 })
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, caches, new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getTransactionReceipt", [hash1])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.receiptReader.read(TransactionId.from(hash1))
                .block(Duration.ofSeconds(1))
                .with { new String(it.data) }
        then:
        act == '{"blockHash":"0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5","blockNumber":"0x64","transactionHash":"0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5","logs":[]}'
    }

    def "Produce empty on non-existing tx"() {
        setup:
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getTransactionByHash", [hash1])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(null), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.txReader.read(TransactionId.from(hash1))
        then:
        StepVerifier.create(act)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads balance - height is unknown"() {
        setup:
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> null
            }
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBalance", [address1, "latest"])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes("0x100"), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.balanceReader.read(Address.from(address1)).map {it.data}
        then:
        StepVerifier.create(act)
                .expectNext(Wei.from("0x100"))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads balance - height is known"() {
        setup:
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> 11_061_691
            }
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBalance", [address1, "0xa8c9bb"])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes("0x100"), null, 1, resolver
                        )
                )
            }
        }
        when:
        def act = reader.balanceReader.read(Address.from(address1)).map {it.data}
        then:
        StepVerifier.create(act)
                .expectNext(Wei.from("0x100"))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads block by hash with retries"() {
        setup:
        def json = new BlockJson().tap {
            number = 100
            parentHash = BlockHash.from(hash1)
            hash = BlockHash.from(hash1)
            timestamp = Instant.now()
            totalDifficulty = BigInteger.ONE
            transactions = []
        }
        def up = Mock(Multistream) {
            3 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            3 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        def result = Mono.just(
                new QuorumRpcReader.Result(
                        Global.objectMapper.writeValueAsBytes(json), null, 1, resolver)
        )
        EthereumDirectReader ethereumDirectReader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        ethereumDirectReader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            2 * create(_, _, _, _) >> Mock(QuorumReader) {
                2 * read(new JsonRpcRequest("eth_getBlockByHash", [hash1, false])) >>>
                        [Mono.error(new RuntimeException()), Mono.error(new RuntimeException())]
            }
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBlockByHash", [hash1, false])) >> result
            }
        }
        when:
        def act = ethereumDirectReader.blockReader.read(BlockHash.from(hash1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.data.hash.toHexWithPrefix() == hash1
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads block by number with retries"() {
        setup:
        def json = new BlockJson().tap {
            number = 100
            hash = BlockHash.from(hash1)
            timestamp = Instant.now()
            totalDifficulty = BigInteger.ONE
            parentHash = BlockHash.from(hash1)
            transactions = []
        }
        def up = Mock(Multistream) {
            3 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            3 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        def result = Mono.just(
                new QuorumRpcReader.Result(
                        Global.objectMapper.writeValueAsBytes(json), null, 1, resolver)
        )
        EthereumDirectReader ethereumDirectReader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        ethereumDirectReader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            2 * create(_, _, _, _) >> Mock(QuorumReader) {
                2 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x64", false])) >>>
                        [Mono.error(new RuntimeException()), Mono.error(new RuntimeException())]
            }
            1 * create(_, _, _, _) >> Mock(QuorumReader) {
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x64", false])) >> result
            }
        }
        when:
        def act = ethereumDirectReader.blockByHeightReader.read(100)
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.data.hash.toHexWithPrefix() == hash1
                }
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads balance with retries - expects an error within 1 sec"() {
        setup:
        def up = Mock(Multistream) {
            4 * getApiSource(_) >> Stub(ApiSource)
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> null
            }
        }
        def calls = Mock(Factory) {
            4 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            4 * create(_, _, _, _) >> Mock(QuorumReader) {
                4 * read(new JsonRpcRequest("eth_getBalance", [address1, "latest"])) >>>
                        [Mono.error(new RuntimeException()), Mono.error(new RuntimeException()),
                         Mono.error(new RuntimeException()), Mono.error(new RuntimeException())]
            }
        }
        when:
        def act = reader.balanceReader.read(Address.from(address1))
        then:
        StepVerifier.create(act)
                .expectError()
                .verify(Duration.ofSeconds(1))
    }

}
