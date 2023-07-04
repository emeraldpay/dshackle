package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsMessage
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

class WsSubscriptionsImplTest : ShouldSpec({

    should("make a subscription") {
        val answers = Flux.fromIterable(
            listOf(
                JsonRpcWsMessage("100".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("101".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("102".toByteArray(), null, "0xcff45d00e7"),
            )
        )
        val conn = mockk<WsConnection>()
        every { conn.callRpc(match { req -> req.method == "eth_subscribe" && req.params == listOf("foo_bar") }) } returns Mono.just(JsonRpcResponse("\"0xcff45d00e7\"".toByteArray(), null))
        every { conn.callRpc(match { req -> req.method == "eth_unsubscribe" }) } returns Mono.just(JsonRpcResponse("true".toByteArray(), null))
        every { conn.getSubscribeResponses() } returns answers
        val pool = mockk<WsConnectionPool>()
        every { pool.getConnection() } returns conn

        val ws = WsSubscriptionsImpl(pool)
        val act = ws.subscribe("foo_bar")
            .map { String(it) }
            .take(3)
            .collectList().block(Duration.ofSeconds(1))

        act shouldBe listOf("100", "101", "102")
    }

    should("unsubscribe when processes requested elements") {
        val answers = Flux.fromIterable(
            listOf(
                JsonRpcWsMessage("100".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("101".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("102".toByteArray(), null, "0xcff45d00e7"),
            )
        )
        val conn = mockk<WsConnection>()
        every { conn.callRpc(match { req -> req.method == "eth_subscribe" && req.params == listOf("foo_bar") }) } returns Mono.just(JsonRpcResponse("\"0xcff45d00e7\"".toByteArray(), null))
        every { conn.callRpc(match { req -> req.method == "eth_unsubscribe" }) } returns Mono.just(JsonRpcResponse("true".toByteArray(), null))
        every { conn.getSubscribeResponses() } returns answers
        val pool = mockk<WsConnectionPool>()
        every { pool.getConnection() } returns conn

        val ws = WsSubscriptionsImpl(pool)
        val act = ws.subscribe("foo_bar")
            .map { String(it) }
            .take(2)
            .collectList().block(Duration.ofSeconds(1))

        // requested only 2 elements though it produces 3, and then cancel which should make eth_unsubscribe
        act shouldBe listOf("100", "101")
        verify(exactly = 1) { conn.callRpc(match { req -> req.method == "eth_unsubscribe" }) }
    }

    should("produce only messages to the actual subscription") {
        val answers = Flux.fromIterable(
            listOf(
                JsonRpcWsMessage("100".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("AAA".toByteArray(), null, "0x000001a0e7"),
                JsonRpcWsMessage("101".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("BBB".toByteArray(), null, "0x000001a0e7"),
                JsonRpcWsMessage("CCC".toByteArray(), null, "0x000001a0e7"),
                JsonRpcWsMessage("102".toByteArray(), null, "0xcff45d00e7"),
                JsonRpcWsMessage("DDD".toByteArray(), null, "0x000001a0e7"),
            )
        )
        val conn = mockk<WsConnection>()
        every { conn.callRpc(match { req -> req.method == "eth_subscribe" && req.params == listOf("foo_bar") }) } returns Mono.just(JsonRpcResponse("\"0xcff45d00e7\"".toByteArray(), null))
        every { conn.callRpc(match { req -> req.method == "eth_unsubscribe" }) } returns Mono.just(JsonRpcResponse("true".toByteArray(), null))
        every { conn.getSubscribeResponses() } returns answers
        val pool = mockk<WsConnectionPool>()
        every { pool.getConnection() } returns conn

        val ws = WsSubscriptionsImpl(pool)
        val act = ws.subscribe("foo_bar")
            .map { String(it) }
            .take(3)
            .collectList().block(Duration.ofSeconds(1))

        act shouldBe listOf("100", "101", "102")
    }
})
