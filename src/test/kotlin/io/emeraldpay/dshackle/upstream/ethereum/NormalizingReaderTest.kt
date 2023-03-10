package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
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

    context("eth_getBlockByHash") {
        val reader = NormalizingReader(AtomicReference(EmptyHead()), Caches.default(), fullBlock)

        should("Use Full Blocks to rebuild transactions when requested") {
            every { hashReader.read(blockId) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHexWithPrefix(), true)))
                .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
        }

        should("Return nothing is full block not requested") {
            val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHexWithPrefix(), false)))
                .block(Duration.ofSeconds(1))

            act shouldBe null
        }
    }

    context("eth_getBlockByNumber") {

        should("Use Full Blocks to rebuild transactions when requested and no height is cached") {
            val reader = NormalizingReader(AtomicReference(EmptyHead()), Caches.default(), fullBlock)
            every { heightReader.read(101) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByNumber", listOf("0x65", true)))
                .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
        }

        should("Use Full Blocks to rebuild transactions when requested and height is cached") {
            val caches = Caches.default()
            caches.memoizeBlock(BlockContainer.from(blockJson))
            val reader = NormalizingReader(AtomicReference(EmptyHead()), caches, fullBlock)

            every { hashReader.read(blockId) } returns
                Mono.just(BlockContainer.from(blockJson))

            val act = reader.read(DshackleRequest("eth_getBlockByNumber", listOf("0x65", true)))
                .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
        }
    }
})
