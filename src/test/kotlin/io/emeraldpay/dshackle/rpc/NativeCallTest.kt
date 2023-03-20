package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.grpc.Chain
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.ints.exactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.instanceOf
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.commons.codec.binary.Hex
import reactor.core.Exceptions
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.lang.RuntimeException
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
        val ctx = NativeCall.CallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("test", emptyList()))

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
        val ctx = NativeCall.CallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("test", emptyList()))

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
        val ctx = NativeCall.CallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("test", emptyList()))

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
        val ctx = NativeCall.CallContext(1, null, upstream, Selector.empty, NativeCall.ParsedCallDetails("eth_test", emptyList()))

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
        should("prepare call as error for invalid blockchain") {
            val native = create()

            val act = shouldThrowAny {
                native.prepareCall(BlockchainOuterClass.NativeCallRequest.getDefaultInstance())
                    .collectList().block(Duration.ofSeconds(1))
            }.let(Exceptions::unwrap)

            act shouldBe instanceOf<NativeCall.CallFailure>()
            (act as NativeCall.CallFailure).reason shouldBe instanceOf<SilentException>()
        }

        should("prepare call as error for unavaialble blockchain") {
            val holder = mockk<MultistreamHolder>()
            every { holder.observeChains() } returns Flux.empty()
            every { holder.isAvailable(Chain.BITCOIN) } returns false
            val native = NativeCall(holder)

            val act = shouldThrowAny {
                native.prepareCall(BlockchainOuterClass.NativeCallRequest.newBuilder().setChain(Common.ChainRef.CHAIN_BITCOIN).build())
                    .collectList().block(Duration.ofSeconds(1))
            }.let(Exceptions::unwrap)

            act shouldBe instanceOf<NativeCall.CallFailure>()
            (act as NativeCall.CallFailure).reason shouldBe instanceOf<SilentException>()

            verify(exactly = 1) { holder.isAvailable(Chain.BITCOIN) }
        }

        should("Parse just a method") {
            val native = create()

            val act = native.parseParams(
                NativeCall.CallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", ""))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe emptyList()
        }

        should("Parse empty params") {
            val native = create()

            val act = native.parseParams(
                NativeCall.CallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", "[]"))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe emptyList()
        }

        should("Parse single param") {
            val native = create()

            val act = native.parseParams(
                NativeCall.CallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", "[1]"))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe listOf(1)
        }

        should("Parse two params") {
            val native = create()

            val act = native.parseParams(
                NativeCall.CallContext(1, null, mockk(), Selector.empty, NativeCall.RawCallDetails("test", "[1, \"hello\"]"))
            )

            act.payload.method shouldBe "test"
            act.payload.params shouldBe listOf(1, "hello")
        }
    }

    context("Response") {
        should("build signature") {
            val native = create()

            val act = native.buildSignature(100, ResponseSigner.Signature(Hex.decodeHex("010203"), "test1", 100))

            act.nonce shouldBe 100
            act.signature.toByteArray().let(Hex::encodeHexString) shouldBe "010203"
            act.upstreamId shouldBe "test1"
            act.keyId shouldBe 100
        }

        should("build basic success response") {
            val native = create()
            val act = native.buildResponse(
                NativeCall.CallResult.ok(1051, null, "true".toByteArray(), null)
            )

            act.succeed shouldBe true
            act.id shouldBe 1051
            act.payload.toStringUtf8() shouldBe "true"
            act.hasSignature() shouldBe false
            act.errorMessageBytes.isEmpty shouldBe true
        }

        should("build success response with signature") {
            val native = create()
            val act = native.buildResponse(
                NativeCall.CallResult.ok(1051, 65432, "true".toByteArray(), ResponseSigner.Signature(Hex.decodeHex("010203"), "test1", 100))
            )

            act.succeed shouldBe true
            act.id shouldBe 1051
            act.payload.toStringUtf8() shouldBe "true"
            act.hasSignature() shouldBe true
            act.signature.let { signature ->
                signature.nonce shouldBe 65432
                signature.signature.toByteArray().let(Hex::encodeHexString) shouldBe "010203"
                signature.upstreamId shouldBe "test1"
                signature.keyId shouldBe 100
            }
            act.errorMessageBytes.isEmpty shouldBe true
        }

        should("build basic fail response") {
            val native = create()
            val act = native.buildResponse(
                NativeCall.CallResult.fail(1051, null, NativeCall.CallError(-32000, "test error", null))
            )

            act.succeed shouldBe false
            act.id shouldBe 1051
            act.payload.isEmpty shouldBe true
            act.hasSignature() shouldBe false
            act.errorMessageBytes.isEmpty shouldBe false
            act.errorMessage shouldBe "test error"
        }
    }

    context("CallError") {
        should("build from JsonRpcException") {
            val act = NativeCall.CallError.from(JsonRpcException(JsonRpcResponse.NumberId(1), JsonRpcError(-32000, "test")))

            act.id shouldBe 1
            act.message shouldBe "test"
            act.upstreamError shouldNotBe null
            act.upstreamError!!.code shouldBe -32000
            act.upstreamError!!.message shouldBe "test"
        }

        should("build from RpcException") {
            val act = NativeCall.CallError.from(RpcException(-32123, "test msg", listOf("internal")))

            act.id shouldNotBe -32123 // we don't have any info on what ID it supposed to be
            act.message shouldBe "test msg"
            act.upstreamError shouldNotBe null
            act.upstreamError!!.code shouldBe -32123
            act.upstreamError!!.message shouldBe "test msg"
        }

        should("build from CallFailure with no details") {
            val act = NativeCall.CallError.from(NativeCall.CallFailure(100, RuntimeException("test")))

            act.id shouldBe 100
            act.message shouldBe "test"
            act.upstreamError shouldBe null
        }

        should("build from CallFailure with details") {
            val act = NativeCall.CallError.from(NativeCall.CallFailure(100, RpcException(-32123, "test msg", listOf("internal"))))

            act.id shouldBe 100
            act.message shouldBe "test msg"
            act.upstreamError shouldNotBe null
            act.upstreamError!!.code shouldBe -32123
            act.upstreamError!!.message shouldBe "test msg"
        }
    }

    context("processPreparedCall") {
        should("execute call") {
            val native = mockk<NativeCall>()
            val call = NativeCall.CallContext(1, null, mockk(relaxed = true), Selector.empty, NativeCall.RawCallDetails("test", "[]"))
            val answer = NativeCall.CallResult.ok(1, null, "ok".toByteArray(), null)

            every { native.processPreparedCall(any()) } answers { callOriginal() }
            every { native.parseParams(any()) } returns call.withPayload(NativeCall.ParsedCallDetails("test", emptyList()))
            every { native.execute(any()) } returns Mono.just(answer)

            val act = native.processPreparedCall(call)
                .block(Duration.ofSeconds(1))

            act shouldBe answer
        }
    }
})
