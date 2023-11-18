package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import reactor.core.publisher.Mono
import java.time.Duration

class BitcoinCacheUpdateTest : ShouldSpec({

    should("Do nothing on non-cacheable call") {
        val cache = mockk<Caches>()
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("getblockcount", emptyList(), 1)
        val response = JsonRpcResponse.ok("123")
        val cacheUpdate = BitcoinCacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp?.resultAsRawString shouldBe "123"
        verify { cache wasNot called }
    }

    should("Cache block from getblock \"") {
        val cache = mockk<Caches>(relaxed = true)
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("getblock", listOf("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"), 1)
        val responseJson = this.javaClass.getResourceAsStream("/bitcoin/block-626472.json").readAllBytes()
        val response = JsonRpcResponse.ok(responseJson)
        val cacheUpdate = BitcoinCacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp.hasResult() shouldBe true
        verify(atLeast = 1) {
            cache.cache(
                Caches.Tag.REQUESTED,
                match<BlockContainer> {
                    it.height == 626472L &&
                        it.hash == BlockId.from("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90")
                }
            )
        }
    }
})
