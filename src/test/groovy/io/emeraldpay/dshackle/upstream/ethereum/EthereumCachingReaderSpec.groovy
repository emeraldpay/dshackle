package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.reader.RequestReader
import io.emeraldpay.dshackle.reader.RequestReaderFactory
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionLogJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionReceiptJson
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBlockByHash", new ListParams([hash1, false]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null)
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBlockByHash", new ListParams([hash1, false]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(null), null, 1, resolver, null
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBlockByNumber", new ListParams(["0x64", false]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null
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

    def "Reads logs by block hash"() {
        setup:
        def json = new TransactionLogJson().tap {
            address = Address.from(address1)
            blockHash = BlockHash.from(hash1)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getLogs", new ListParams([Map.of("blockHash", hash1)]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes([json]), null, 1, resolver, null
                        )
                )
            }
        }
        when:
        def act = reader.logsByHashReader.read(BlockId.from(hash1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { logs ->
                    logs.data[0].blockHash == BlockHash.from(hash1)
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getTransactionByHash", new ListParams([hash1]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getTransactionReceipt", new ListParams([hash1]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        def caches = Mock(Caches) {
            // note that the Caches needs a Height value, otherwise it's not cached
            1 * cacheReceipt(Caches.Tag.REQUESTED, { DefaultContainer data -> data.txId.toHex() == hash1.substring(2) && data.height == 100 })
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), caches, new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getTransactionReceipt", new ListParams([hash1]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null
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
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getTransactionByHash", new ListParams([hash1]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes(null), null, 1, resolver, null
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
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> null
            }
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBalance", new ListParams([address1, "latest"]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes("0x100"), null, 1, resolver, null
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
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> 11_061_691
            }
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBalance", new ListParams([address1, "0xa8c9bb"]))) >> Mono.just(
                        new RequestReader.Result(
                                Global.objectMapper.writeValueAsBytes("0x100"), null, 1, resolver, null
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
        def calls = Mock(Factory) {
            3 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        def result = Mono.just(
                new RequestReader.Result(
                        Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null)
        )
        EthereumDirectReader ethereumDirectReader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        ethereumDirectReader.requestReaderFactory = Mock(RequestReaderFactory) {
            2 * create(_) >> Mock(RequestReader) {
                2 * read(new ChainRequest("eth_getBlockByHash", new ListParams([hash1, false]))) >>>
                        [Mono.error(new RuntimeException()), Mono.error(new RuntimeException())]
            }
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBlockByHash", new ListParams([hash1, false]))) >> result
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
        def calls = Mock(Factory) {
            3 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        def result = Mono.just(
                new RequestReader.Result(
                        Global.objectMapper.writeValueAsBytes(json), null, 1, resolver, null)
        )
        EthereumDirectReader ethereumDirectReader = new EthereumDirectReader(
                Stub(Multistream), Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        ethereumDirectReader.requestReaderFactory = Mock(RequestReaderFactory) {
            2 * create(_) >> Mock(RequestReader) {
                2 * read(new ChainRequest("eth_getBlockByNumber", new ListParams(["0x64", false]))) >>>
                        [Mono.error(new RuntimeException()), Mono.error(new RuntimeException())]
            }
            1 * create(_) >> Mock(RequestReader) {
                1 * read(new ChainRequest("eth_getBlockByNumber", new ListParams(["0x64", false]))) >> result
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
            1 * getHead() >> Mock(Head) {
                1 * getCurrentHeight() >> null
            }
        }
        def calls = Mock(Factory) {
            4 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM__MAINNET, false)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls, TestingCommons.tracerMock()
        )
        reader.requestReaderFactory = Mock(RequestReaderFactory) {
            4 * create(_) >> Mock(RequestReader) {
                4 * read(new ChainRequest("eth_getBalance", new ListParams([address1, "latest"]))) >>>
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
