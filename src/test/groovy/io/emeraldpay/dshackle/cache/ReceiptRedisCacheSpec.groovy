package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.IntegrationTestingCommons
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.lettuce.core.api.StatefulRedisConnection
import org.testcontainers.containers.GenericContainer
import org.testcontainers.spock.Testcontainers
import org.testcontainers.utility.DockerImageName
import spock.lang.IgnoreIf
import spock.lang.Specification

@Testcontainers
class ReceiptRedisCacheSpec extends Specification {

    GenericContainer redisContainer = new GenericContainer(
            DockerImageName.parse("redis:5.0.3-alpine")
    ).withExposedPorts(6379)

    StatefulRedisConnection<String, byte[]> redis
    ReceiptRedisCache cache
    ObjectMapper objectMapper = Global.objectMapper

    String receiptJson = '''
    {
        "blockHash": "0x2c3cfd4c7f2b58859371f5795eaf8524caa6e63145ac7e9df23c8d63aab891ae",
        "blockNumber": "0x213b8a",
        "contractAddress": null,
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logs": [],
        "transactionHash": "0x5929b36be4586c57bd87dfb7ea6be3b985c1f527fa3d69d221604b424aeb4197",
        "transactionIndex": "0x00"
      }
    '''
    TransactionReceiptJson receipt = new TransactionReceiptJson().tap {
        transactionHash = TransactionId.from("0x5929b36be4586c57bd87dfb7ea6be3b985c1f527fa3d69d221604b424aeb4197")
        transactionIndex = 0
        blockHash = BlockHash.from("0x2c3cfd4c7f2b58859371f5795eaf8524caa6e63145ac7e9df23c8d63aab891ae")
        blockNumber = 0x213b8a
        cumulativeGasUsed = 0x5208
        gasUsed = 0x5208
        logs = []
    }

    def setup() {
        redis = IntegrationTestingCommons.redisConnection(redisContainer.firstMappedPort)
        redis.sync().flushdb()
        cache = new ReceiptRedisCache(
                redis.reactive(), Chain.ETHEREUM
        )
    }

    def cleanup() {
        redis.close()
    }

    def "Add and read"() {
        setup:

        def container = new DefaultContainer(
                TxId.from(receipt.transactionHash),
                BlockId.from(receipt.blockHash),
                receipt.blockNumber,
                receiptJson.bytes,
                receipt
        )

        when:
        cache.add(container).block()
        def act = cache.read(TxId.from(receipt.transactionHash)).block()
        then:
        act != null
        objectMapper.readValue(act, TransactionReceiptJson) == receipt
    }

    def "Evict value"() {
        setup:

        def container = new DefaultContainer(
                TxId.from(receipt.transactionHash),
                BlockId.from(receipt.blockHash),
                receipt.blockNumber,
                receiptJson.bytes,
                receipt
        )

        when:
        cache.add(container).block()
        def act = cache.read(TxId.from(receipt.transactionHash)).block()
        then:
        act != null

        when:
        cache.evict(TxId.from(receipt.transactionHash)).subscribe()
        act = cache.read(TxId.from(receipt.transactionHash)).block()
        then:
        act == null
    }
}
