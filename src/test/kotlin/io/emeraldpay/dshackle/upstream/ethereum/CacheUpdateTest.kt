package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import reactor.core.publisher.Mono
import java.time.Duration

class CacheUpdateTest : ShouldSpec({

    should("Do nothing on non-cacheable call") {
        val cache = mockk<Caches>()
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("eth_blockNumber", emptyList(), 1)
        val response = JsonRpcResponse.ok("\"0x123\"")
        val cacheUpdate = CacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp?.resultAsProcessedString shouldBe "0x123"
        verify { cache wasNot called }
    }

    should("Cache block from eth_getBlockByNumber") {
        val cache = mockk<Caches>(relaxed = true)
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("eth_getBlockByNumber", listOf("0x123", false), 1)
        val responseJson = this.javaClass.getResourceAsStream("/ethereum/block-291.json").readAllBytes()
        val response = JsonRpcResponse.ok(responseJson)
        val cacheUpdate = CacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp.hasResult() shouldBe true
        verify(atLeast = 1) {
            cache.cache(
                Caches.Tag.REQUESTED,
                match<BlockContainer> {
                    it.height == 0x123L &&
                        it.hash == BlockId.from("0xc5dab4e189004a1312e9db43a40abb2de91ad7dd25e75880bf36016d8e9df524")
                }
            )
        }
    }

    should("Cache block from eth_getBlockByHash") {
        val cache = mockk<Caches>(relaxed = true)
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("eth_getBlockByHash", listOf("0xc5dab4e189004a1312e9db43a40abb2de91ad7dd25e75880bf36016d8e9df524", false), 1)
        val responseJson = this.javaClass.getResourceAsStream("/ethereum/block-291.json").readAllBytes()
        val response = JsonRpcResponse.ok(responseJson)
        val cacheUpdate = CacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp.hasResult() shouldBe true
        verify(atLeast = 1) {
            cache.cache(
                Caches.Tag.REQUESTED,
                match<BlockContainer> {
                    it.height == 0x123L &&
                        it.hash == BlockId.from("0xc5dab4e189004a1312e9db43a40abb2de91ad7dd25e75880bf36016d8e9df524")
                }
            )
        }
    }

    should("Cache tx from eth_getTransactionByHash") {
        val cache = mockk<Caches>(relaxed = true)
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("eth_getTransactionByHash", listOf("0xcb13faa6174ee9c1a21540cae32dd64ae6b3bc814b66ce5ed6843e65d112e391"), 1)
        val responseJson = this.javaClass.getResourceAsStream("/ethereum/tx-cb13fa.json").readAllBytes()
        val response = JsonRpcResponse.ok(responseJson)
        val cacheUpdate = CacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp.hasResult() shouldBe true
        verify(atLeast = 1) {
            cache.cache(
                Caches.Tag.REQUESTED,
                match<TxContainer> {
                    it.height == 0x123456L &&
                        it.hash == TxId.from("0xcb13faa6174ee9c1a21540cae32dd64ae6b3bc814b66ce5ed6843e65d112e391")
                }
            )
        }
    }

    should("Cache tx receipt from eth_getTransactionReceipt") {
        val cache = mockk<Caches>(relaxed = true)
        val delegate = mockk<StandardRpcReader>()

        val request = JsonRpcRequest("eth_getTransactionReceipt", listOf("0xcb13faa6174ee9c1a21540cae32dd64ae6b3bc814b66ce5ed6843e65d112e391"), 1)
        val responseJson = this.javaClass.getResourceAsStream("/ethereum/receipt-cb13fa.json").readAllBytes()
        val response = JsonRpcResponse.ok(responseJson)
        val cacheUpdate = CacheUpdate(cache, delegate)

        every { delegate.read(request) } returns Mono.just(response)
        val resp = cacheUpdate.read(request).block(Duration.ofSeconds(1))

        resp.hasResult() shouldBe true
        verify(atLeast = 1) {
            cache.cacheReceipt(
                Caches.Tag.REQUESTED,
                match<DefaultContainer<TransactionReceiptJson>> {
                    it.txId == TxId.from("0xcb13faa6174ee9c1a21540cae32dd64ae6b3bc814b66ce5ed6843e65d112e391")
                }
            )
        }
    }
})
