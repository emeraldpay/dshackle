package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.TxMemCache
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldEndWith
import io.kotest.matchers.string.shouldStartWith
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.mockk
import org.apache.commons.codec.binary.Hex
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.Duration
import java.time.Instant

class EthereumFullBlocksReaderTest : ShouldSpec({

    // sorted
    val hash1 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    val hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    val hash3 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"
    val hash4 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"

    val objectMapper = Global.objectMapper

    val tx1 = TransactionJson().also {
        it.blockNumber = 100
        it.blockHash = BlockHash.from(hash1)
        it.hash = TransactionId.from(hash1)
        it.nonce = 1
    }
    val tx2 = TransactionJson().also {
        it.blockNumber = 100
        it.blockHash = BlockHash.from(hash1)
        it.hash = TransactionId.from(hash2)
        it.nonce = 2
    }
    val tx3 = TransactionJson().also {
        it.blockNumber = 101
        it.blockHash = BlockHash.from(hash3)
        it.hash = TransactionId.from(hash3)
        it.nonce = 3
    }

    // no block
    val tx4 = TransactionJson().also {
        it.blockNumber = 102
        it.blockHash = BlockHash.from(hash4)
        it.hash = TransactionId.from(hash4)
        it.nonce = 4
    }

    // two transactions, tx1 and tx2
    val block1 = BlockJson<TransactionRefJson>().also {
        it.number = 100
        it.hash = BlockHash.from(hash1)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = listOf(
            TransactionRefJson(tx1.hash),
            TransactionRefJson(tx2.hash)
        )
    }

    // one transaction, tx3
    val block2 = BlockJson<TransactionRefJson>().also {
        it.number = 101
        it.hash = BlockHash.from(hash3)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = listOf(
            TransactionRefJson(tx3.hash)
        )
    }

    // no transactions
    val block3 = BlockJson<TransactionRefJson>().also {
        it.number = 102
        it.hash = BlockHash.from(hash4)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = emptyList()
    }

    // all transaction2
    val block4 = BlockJson<TransactionRefJson>().also {
        it.number = 103
        it.hash = BlockHash.from(hash3)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = listOf(
            TransactionRefJson(tx1.hash),
            TransactionRefJson(tx2.hash),
            TransactionRefJson(tx3.hash),
            TransactionRefJson(tx4.hash)
        )
    }

    val txes = TxMemCache().also {
        it.add(TxContainer.from(tx1))
        it.add(TxContainer.from(tx2))
        it.add(TxContainer.from(tx3))
        it.add(TxContainer.from(tx4))
    }
    val blocks = BlocksMemCache().also {
        it.add(BlockContainer.from(block1))
        it.add(BlockContainer.from(block2))
        it.add(BlockContainer.from(block3))
        it.add(BlockContainer.from(block3))
    }

    val full = EthereumFullBlocksReader(blocks, mockk(), txes)

    should("Read two transactions and return full block") {
        val act = full.byHash.read(BlockId.from(block1.hash))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act.json shouldNotBe null

        val block = objectMapper.readValue(act.json, BlockJson::class.java) as BlockJson<TransactionJson>

        block.hash shouldBe BlockHash.from(hash1)
        block.number shouldBe 100
        block.transactions.size shouldBe 2
        block.transactions[0].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash1)
            nonce shouldBe 1
        }
        block.transactions[1].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash2)
            nonce shouldBe 2
        }
    }

    should("Read one transaction and return full block") {
        val act = full.byHash.read(BlockId.from(block2.hash))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act.json shouldNotBe null

        val block = objectMapper.readValue(act.json, BlockJson::class.java) as BlockJson<TransactionJson>

        block.hash shouldBe BlockHash.from(hash3)
        block.number shouldBe 101
        block.transactions.size shouldBe 1
        block.transactions[0].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash3)
            nonce shouldBe 3
        }
    }

    should("Produce block with original order of transactions") {

        val txes = mockk<Reader<TxId, TxContainer>>() {
            every { read(TxId.from(tx1.hash)) } returns Mono.just(TxContainer.from(tx1)).delayElement(Duration.ofMillis(150))
            every { read(TxId.from(tx2.hash)) } returns Mono.just(TxContainer.from(tx2)).delayElement(Duration.ofMillis(100))
            every { read(TxId.from(tx3.hash)) } returns Mono.just(TxContainer.from(tx3)).delayElement(Duration.ofMillis(70))
            every { read(TxId.from(tx4.hash)) } returns Mono.just(TxContainer.from(tx4)).delayElement(Duration.ofMillis(20))
        }
        val blocks = BlocksMemCache().also {
            it.add(BlockContainer.from(block4))
        }

        val full = EthereumFullBlocksReader(blocks, mockk(), txes)

        val act = full.byHash.read(BlockId.from(block4.hash))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act.json shouldNotBe null

        val block = objectMapper.readValue(act.json, BlockJson::class.java) as BlockJson<TransactionJson>
        block.transactions.size shouldBe 4
        block.transactions[0].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash1)
        }
        block.transactions[1].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash2)
        }
        block.transactions[2].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash3)
        }
        block.transactions[3].apply {
            shouldBeInstanceOf<TransactionJson>()
            hash shouldBe TransactionId.from(hash4)
        }
    }

    should("Read block without transactions") {
        val act = full.byHash.read(BlockId.from(block3.hash))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act.json shouldNotBe null

        val block = objectMapper.readValue(act.json, BlockJson::class.java) as BlockJson<TransactionJson>

        block.hash shouldBe BlockHash.from(hash4)
        block.number shouldBe 102
        block.transactions.size shouldBe 0
    }

    should("Return nothing if some of tx are unavailable") {
        val txes = TxMemCache().also {
            it.add(TxContainer.from(tx1))
        }
        val blocks = BlocksMemCache().also {
            it.add(BlockContainer.from(block1)) // missing tx2 in cache
        }
        val full = EthereumFullBlocksReader(blocks, mockk(), txes)

        val act = full.byHash.read(BlockId.from(block1.hash))
            .block(Duration.ofSeconds(1))

        act shouldBe null
    }

    should("Return nothing if no block") {
        val blocks = BlocksMemCache()
        val full = EthereumFullBlocksReader(blocks, mockk(), txes)

        val act = full.byHash.read(BlockId.from(block1.hash))
            .block(Duration.ofSeconds(1))

        act shouldBe null
    }

    context("Process JSONs") {
        should("Not change original cached json") {

            val asPrettyPrint = { json: ByteArray ->
                Global.objectMapper.writer(DefaultPrettyPrinter())
                    .writeValueAsString(Global.objectMapper.readValue(json, Map::class.java))
            }

            val tx1 = """
            {
                "hash": "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
                "blockHash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
                "blockNumber": "0x100001",
                "extraField3": "extraValue3"
            }
            """
            val tx2 = """
            {
                "hash": "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e",
                "blockHash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
                "blockNumber": "0x100001",
                "from": "0x2a65aca4d5fc5b5c859090a6c34d164135398226",
                "extraField4": "extraValue4"
            }
            """

            val blockJson = """
            {
                "number": "0x100001",
                "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
                "timestamp": "0x56cc7b8c",
                "totalDifficulty": "0x6baba0399a0f2e73",
                "extraField": "extraValue",
                "transactions": [
                  "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
                  "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
                ],
                "extraField2": "extraValue2"
            }
            """

            val blockJsonExpected = """
            {
                "number": "0x100001",
                "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
                "timestamp": "0x56cc7b8c",
                "totalDifficulty": "0x6baba0399a0f2e73",
                "extraField": "extraValue",
                "transactions": [
                    $tx1, $tx2 
                ],
                "extraField2": "extraValue2"
            }
            """.let {
                asPrettyPrint(it.toByteArray())
            }

            val txes = TxMemCache().also {
                it.add(TxContainer.from(tx1.toByteArray()))
                it.add(TxContainer.from(tx2.toByteArray()))
            }
            val blocks = BlocksMemCache().also {
                it.add(BlockContainer.fromEthereumJson(blockJson.toByteArray()))
            }

            val full = EthereumFullBlocksReader(blocks, mockk(), txes)
            val act = full.byHash.read(BlockId.from("0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446"))
                .block(Duration.ofSeconds(1)).let {
                    asPrettyPrint(it.json!!)
                }

            act shouldContain "extraValue"
            act shouldContain "extraValue2"
            act shouldContain "extraValue3"
            act shouldContain "extraValue4"
            act shouldBe blockJsonExpected
        }

        should("Split block with tx") {
            val blockJson = """
            {
                "number": "0x100001",
                "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
                "timestamp": "0x56cc7b8c",
                "totalDifficulty": "0x6baba0399a0f2e73",
                "extraField": "extraValue",
                "transactions": [
                  "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
                  "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
                ],
                "extraField2": "extraValue2"
            }
            """
            val reader = EthereumFullBlocksReader.BlockMerge(blocks, mockk())
            val act = reader.splitByTransactions(blockJson.toByteArray())

            act.t1.toString(Charset.defaultCharset()) shouldEndWith "\"transactions\": ["
            act.t2.toString(Charset.defaultCharset()) shouldStartWith "],"
            act.t2.toString(Charset.defaultCharset()).trim() shouldEndWith "}"
        }

        should("Split block with tx formatted with a space") {
            val blockJson = """
            {
                "number": "0x100001",
                "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
                "timestamp": "0x56cc7b8c",
                "totalDifficulty": "0x6baba0399a0f2e73",
                "extraField": "extraValue",
                "transactions" : [
                  "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
                  "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
                ] ,
                "extraField2": "extraValue2"
            }
            """

            val reader = EthereumFullBlocksReader.BlockMerge(blocks, mockk())
            val act = reader.splitByTransactions(blockJson.toByteArray())

            act.t1.toString(Charset.defaultCharset()) shouldEndWith "\"transactions\" : ["
            act.t2.toString(Charset.defaultCharset()) shouldStartWith "] ,"
            act.t2.toString(Charset.defaultCharset()).trim() shouldEndWith "}"
        }

        should("Split block with tx formatted without space") {
            val blockJson = """
                {"extraField": "extraValue","transactions":["0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77","0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"]}
            """.trimIndent()

            val reader = EthereumFullBlocksReader.BlockMerge(blocks, mockk())
            val act = reader.splitByTransactions(blockJson.toByteArray())

            act.t1.toString(Charset.defaultCharset()) shouldEndWith "\"transactions\":["
            act.t2.toString(Charset.defaultCharset()) shouldBe "]}"
        }

        should("Extract from buffer without trailing zeroes") {
            val reader = EthereumFullBlocksReader.BlockMerge(blocks, mockk())

            val act1 = ByteBuffer.allocate(8)
                .put(1)
                .put(2)
                .let(reader::extractContent)

            act1.size shouldBe 2
            Hex.encodeHexString(act1) shouldBe "0102"

            val act2 = ByteBuffer.allocate(8)
                .put(1)
                .put(2)
                .put(3)
                .let(reader::extractContent)

            act2.size shouldBe 3
            Hex.encodeHexString(act2) shouldBe "010203"

            val act3 = ByteBuffer.allocate(8)
                .put(1)
                .put(2)
                .put(3)
                .put(4)
                .put(5)
                .put(6)
                .put(7)
                .put(8)
                .let(reader::extractContent)

            act3.size shouldBe 8
            Hex.encodeHexString(act3) shouldBe "0102030405060708"
        }
    }
})
