package io.emeraldpay.dshackle.rpc

import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.etherjar.rpc.RpcException
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.charset.Charset
import java.time.Duration

class NativeCallTest : ShouldSpec({

    fun create(): NativeCall {
        val holder = mockk<MultistreamHolder>()
        every { holder.observeChains() } returns Flux.empty()
        return NativeCall(holder)
    }

    should("Execute on multistream") {
        val native = create()
        val upstream = mockk<Multistream>()
        val ctx = NativeCall.ValidCallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("test", emptyList()))

        every { upstream.read(DshackleRequest(1, "test", emptyList())) } returns
            Mono.just(DshackleResponse(1, "\"ok\"".toByteArray()))
        val act = native.execute(ctx)
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act!!.error shouldBe null
        act.result shouldNotBe null
        act.result!!.toString(Charset.defaultCharset()) shouldBe "\"ok\""
    }

    should("Return error if a Rpc Exception produced") {
        val native = create()
        val upstream = mockk<Multistream>()
        val ctx = NativeCall.ValidCallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("test", emptyList()))

        every { upstream.read(DshackleRequest(1, "test", emptyList())) } returns
            Mono.error(RpcException(-12345, "Damn!"))

        val act = native.execute(ctx)
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act!!.error shouldNotBe null
        act.result shouldBe null
        act.error!!.message shouldBe "Damn!"
        act.error!!.upstreamError shouldNotBe null
        act.error!!.upstreamError!!.code shouldBe -12345
    }

    should("Return error if a Json Rpc Exception produced") {
        val native = create()
        val upstream = mockk<Multistream>()
        val ctx = NativeCall.ValidCallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("test", emptyList()))

        every { upstream.read(DshackleRequest(1, "test", emptyList())) } returns
            Mono.error(JsonRpcException(id = 1, message = "Damn!"))

        val act = native.execute(ctx)
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act!!.error shouldNotBe null
        act.result shouldBe null
        act.error!!.message shouldBe "Damn!"
        act.error!!.upstreamError shouldNotBe null
        act.error!!.upstreamError!!.code shouldBe -32005
    }

    should("Return error if nothing returned from execution") {
        val native = create()
        val upstream = mockk<Multistream>()
        val ctx = NativeCall.ValidCallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("eth_test", emptyList()))

        every { upstream.read(DshackleRequest(1, "eth_test", emptyList())) } returns
            Mono.empty()

        val act = native.execute(ctx)
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act!!.error shouldNotBe null
        act.result shouldBe null
        act.error!!.message shouldBe "No response or no available upstream for eth_test"
    }

    context("Parse input") {
        should("Parse just a method") {
            val native = create()

            val act = native.parseParams(
                NativeCall.ValidCallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", ""))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe emptyList()
        }

        should("Parse empty params") {
            val native = create()

            val act = native.parseParams(
                NativeCall.ValidCallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", "[]"))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe emptyList()
        }

        should("Parse single param") {
            val native = create()

            val act = native.parseParams(
                NativeCall.ValidCallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", "[1]"))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe listOf(1)
        }

        should("Parse two params") {
            val native = create()

            val act = native.parseParams(
                NativeCall.ValidCallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", "[1, \"hello\"]"))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe listOf(1, "hello")
        }
    }
})
