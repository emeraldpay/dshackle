package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.test.FixedHead
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class NormalizingReaderTest : ShouldSpec({

    val blockId = BlockId.from("0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2")
    val blockJson = BlockJson<TransactionRefJson>().also { blockJson ->
        blockJson.hash = BlockHash.from(blockId.value)
        blockJson.totalDifficulty = BigInteger.ONE
        blockJson.number = 101
        blockJson.timestamp = Instant.ofEpochSecond(100000000)
        blockJson.transactions = emptyList()
        blockJson.uncles = emptyList()
    }

    val heightReader = mockk<Reader<Long, BlockContainer>>()
    val hashReader = mockk<Reader<BlockId, BlockContainer>>()
    val fullBlock = mockk<EthereumFullBlocksReader>()
    every { fullBlock.byHeight } returns heightReader
    every { fullBlock.byHash } returns hashReader

    context("Predicated") {
        should("Accept block") {
            val reader = NormalizingReader(AtomicReference(EmptyHead()), Caches.default(), fullBlock)

            reader.isBlockOrNumber("0x9e1891c70836f4d5817924a801e3e74fdd794842dfb762ff6767db503208a409") shouldBe true
            reader.isBlockOrNumber("0x515846D0F6CC07C99B60F6A910723D5C91817E3C3570D198427222D4ED9F4FB7") shouldBe true
        }

        should("Accept height") {
            val reader = NormalizingReader(AtomicReference(EmptyHead()), Caches.default(), fullBlock)

            reader.isBlockOrNumber("0x0") shouldBe true
            reader.isBlockOrNumber("0x1D4C00") shouldBe true
            reader.isBlockOrNumber("0x1d4c00") shouldBe true
            reader.isBlockOrNumber("0x1112e74") shouldBe true
        }

        should("Deny invalid block") {
            val reader = NormalizingReader(AtomicReference(EmptyHead()), Caches.default(), fullBlock)

            reader.isBlockOrNumber("0x9e1891c70836f4d5817924") shouldBe false
            reader.isBlockOrNumber("0x515846D0F6CC07C99B60F6A910723D5C91817E3C3570D198427222D4ED9F4FB") shouldBe false
            reader.isBlockOrNumber("foobar") shouldBe false
        }

        should("Accept latest tag block") {
            val head = FixedHead(height = 100)
            val reader = NormalizingReader(AtomicReference(head), Caches.default(), fullBlock)

            reader.acceptBlock("latest") shouldBe true
        }

        should("Accept known recent block") {
            val head = FixedHead(block = BlockContainer.from(blockJson))
            val caches = Caches.default()
            caches.memoizeBlock(BlockContainer.from(blockJson))
            val reader = NormalizingReader(AtomicReference(head), caches, fullBlock)

            reader.acceptBlock("0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2") shouldBe true
        }

        should("Deny unknown block") {
            val head = FixedHead(block = BlockContainer.from(blockJson))
            val caches = Caches.default()
            caches.memoizeBlock(BlockContainer.from(blockJson))
            val reader = NormalizingReader(AtomicReference(head), caches, fullBlock)

            reader.acceptBlock("0x720c6f4ecac5f8f7db001be032878b8ab2f85b826fdf98ee0f4e63ceb0568404") shouldBe false
        }

        should("Accept recent block") {
            val head = FixedHead(height = 100)
            val reader = NormalizingReader(AtomicReference(head), Caches.default(), fullBlock)

            reader.acceptHeight(100) shouldBe true
            reader.acceptHeight(99) shouldBe true
            reader.acceptHeight(98) shouldBe true
            reader.acceptHeight(97) shouldBe true
            reader.acceptHeight(96) shouldBe true
            reader.acceptHeight(95) shouldBe true
        }

        should("Accept future block") {
            val head = FixedHead(height = 100)
            val reader = NormalizingReader(AtomicReference(head), Caches.default(), fullBlock)

            reader.acceptHeight(105) shouldBe true
        }

        should("Deny old block") {
            val head = FixedHead(height = 100)
            val reader = NormalizingReader(AtomicReference(head), Caches.default(), fullBlock)

            reader.acceptHeight(75) shouldBe false
            reader.acceptHeight(0) shouldBe false
        }
    }

    context("eth_getBlockByHash") {
        val head = FixedHead(block = BlockContainer.from(blockJson))
        val caches = Caches.default()
        caches.memoizeBlock(BlockContainer.from(blockJson))
        val reader = NormalizingReader(AtomicReference(head), caches, fullBlock)

        should("Use Full Blocks to rebuild transactions when requested") {
            every { hashReader.read(blockId) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHexWithPrefix(), true)))
                .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
        }

        should("Return nothing when old block requests") {
            head.height = 200
            every { hashReader.read(blockId) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHexWithPrefix(), true)))
                .block(Duration.ofSeconds(1))

            act shouldBe null
        }

        should("Return nothing is full block not requested") {
            val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHexWithPrefix(), false)))
                .block(Duration.ofSeconds(1))

            act shouldBe null
        }
    }

    context("eth_getBlockByNumber") {

        should("Use Full Blocks to rebuild transactions when requested and no height is cached") {
            val head = FixedHead(height = 101)
            val reader = NormalizingReader(AtomicReference(head), Caches.default(), fullBlock)
            every { heightReader.read(101) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByNumber", listOf("0x65", true)))
                .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
        }

        should("Use Full Blocks to rebuild transactions when requested and height is cached") {
            val head = FixedHead(height = 101)
            val caches = Caches.default()
            caches.memoizeBlock(BlockContainer.from(blockJson))
            val reader = NormalizingReader(AtomicReference(head), caches, fullBlock)

            every { hashReader.read(blockId) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByNumber", listOf("0x65", true)))
                .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
        }

        should("Return nothing when old block requests") {
            val head = FixedHead(height = 101)
            val reader = NormalizingReader(AtomicReference(head), Caches.default(), fullBlock)

            val act = reader.read(DshackleRequest("eth_getBlockByNumber", listOf("0x25", true)))
                .block(Duration.ofSeconds(1))

            act shouldBe null
        }
    }
})
