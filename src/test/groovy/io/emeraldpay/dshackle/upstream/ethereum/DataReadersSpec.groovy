package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.reader.QuorumRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.ApiSource
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.emeraldpay.api.Chain
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import org.apache.commons.collections4.Factory
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class DataReadersSpec extends Specification {

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
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getBlockByHash", [hash1, false], json)
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

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

    def "Produce empty result on non-existing block"() {
        setup:
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getBlockByHash", [hash1, false], null)
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

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
            transactions = []
        }
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getBlockByNumber", ["0x64", false], json)
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

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
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getTransactionByHash", [hash1], json)
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

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

    def "Reads tx receipt"() {
        setup:
        def json = new TransactionReceiptJson().tap {
            transactionHash = TransactionId.from(hash1)
            blockNumber = 100
            blockHash = BlockHash.from(hash1)
        }
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getTransactionReceipt", [hash1], json)
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

        when:
        def act = reader.receiptReader.read(TransactionId.from(hash1))
            .block(Duration.ofSeconds(1))
            .with { new String(it) }
        then:
        act.contains('"blockHash":"0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"')
        act.contains('"transactionHash":"0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"')
        act.contains('"blockNumber":"0x64"')
    }

    def "Produce empty on non-existing tx"() {
        setup:
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getTransactionByHash", [hash1], null)
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

        when:
        def act = reader.txReader.read(TransactionId.from(hash1))
        then:
        StepVerifier.create(act)
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Reads balance - height is unknown"() {
        setup:
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getBalance", [address1, "latest"], "0x100")
        DataReaders reader = new DataReaders(
                up, new AtomicReference(new EmptyHead())
        )

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

        def head = Mock(Head) {
            1 * getCurrentHeight() >> 11_061_691
        }
        def up = TestingCommons.dshackleApi()
        up.answer("eth_getBalance", [address1, "0xa8c9bb"], "0x100")
        DataReaders reader = new DataReaders(
                up, new AtomicReference(head)
        )

        when:
        def act = reader.balanceReader.read(Address.from(address1))
        then:
        StepVerifier.create(act)
                .expectNext(Wei.from("0x100"))
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

}
