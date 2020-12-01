package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.quorum.QuorumRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import org.apache.commons.collections4.Factory
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class EthereumDirectReaderSpec extends Specification {

    String hash1 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String address1 = "0xe0aadb0a012dbcdc529c4c743d3e0385a0b54d3d"

    def "Reads block by hash"() {
        setup:
        def json = new BlockJson().tap {
            number = 100
            hash = BlockHash.from(hash1)
            timestamp = Instant.now()
            totalDifficulty = BigInteger.ONE
            transactions = []
        }
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_getBlockByHash", [hash1, false])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), 1
                        )
                )
            }
        }
        when:
        def act = reader.blockReader.read(BlockHash.from(hash1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.hash.toHexWithPrefix() == hash1
                }
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
            transactions = []
        }
        def up = Mock(Multistream) {
            1 * getApiSource(_) >> Stub(ApiSource)
        }
        def calls = Mock(Factory) {
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_getBlockByNumber", ["0x64", false])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), 1
                        )
                )
            }
        }
        when:
        def act = reader.blockByHeightReader.read(100)
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.hash.toHexWithPrefix() == hash1
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
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_getTransactionByHash", [hash1])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes(json), 1
                        )
                )
            }
        }
        when:
        def act = reader.txReader.read(TransactionId.from(hash1))
        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.hash.toHexWithPrefix() == hash1
                }
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
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_getBalance", [address1, "latest"])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes("0x100"), 1
                        )
                )
            }
        }
        when:
        def act = reader.balanceReader.read(Address.from(address1))
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
            1 * create() >> new DefaultEthereumMethods(Chain.ETHEREUM)
        }
        EthereumDirectReader reader = new EthereumDirectReader(
                up, Caches.default(), new CurrentBlockCache(), calls
        )
        reader.quorumReaderFactory = Mock(QuorumReaderFactory) {
            1 * create(_, _) >> Mock(Reader) {
                1 * read(new JsonRpcRequest("eth_getBalance", [address1, "0xa8c9bb"])) >> Mono.just(
                        new QuorumRpcReader.Result(
                                Global.objectMapper.writeValueAsBytes("0x100"), 1
                        )
                )
            }
        }
        when:
        def act = reader.balanceReader.read(Address.from(address1))
        then:
        StepVerifier.create(act)
                .expectNext(Wei.from("0x100"))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

}
