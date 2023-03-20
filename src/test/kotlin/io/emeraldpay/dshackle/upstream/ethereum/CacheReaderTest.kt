package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Duration
import java.time.Instant

class CacheReaderTest : ShouldSpec({

    val objectMapper = Global.objectMapper

    val blockId = BlockId.from("f85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2")
    val blockJson = BlockJson<TransactionRefJson>().also { blockJson ->
        blockJson.hash = BlockHash.from(blockId.value)
        blockJson.totalDifficulty = BigInteger.ONE
        blockJson.number = 101
        blockJson.timestamp = Instant.ofEpochSecond(100000000)
        blockJson.transactions = emptyList()
        blockJson.uncles = emptyList()
    }
    val txId = BlockId.from("a38e7b4d456777c94b46c61a1e4cf52fbdd92acc4444719d1fad77005698c221")
    val txJson = TransactionJson().also { json ->
        json.hash = TransactionId.from(txId.value)
        json.blockHash = blockJson.hash
        json.blockNumber = blockJson.number
    }

    should("Read from cache Block by Id ") {
        val memCache = mockk<BlocksMemCache>()
        every { memCache.read(blockId) } returns Mono.just(BlockContainer.from(blockJson))

        val caches = Caches.newBuilder()
            .setBlockByHash(memCache)
            .build()
        val reader = CacheReader(caches)

        val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHex(), false)))
            .block(Duration.ofSeconds(1))

        act.hasResult shouldBe true
        objectMapper.readValue(act.resultOrEmpty, BlockJson::class.java) shouldBe blockJson
    }

    should("Return empty if not cached") {
        val memCache = mockk<BlocksMemCache>()
        every { memCache.read(blockId) } returns Mono.empty()

        val caches = Caches.newBuilder()
            .setBlockByHash(memCache)
            .build()
        val reader = CacheReader(caches)

        val act = reader.read(DshackleRequest("eth_getBlockByHash", listOf(blockId.toHex(), false)))
            .block(Duration.ofSeconds(1))

        act shouldBe null
    }
})
