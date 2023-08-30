package io.emeraldpay.dshackle.rpc

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeCallRequest
import io.emeraldpay.dshackle.Global
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.springframework.util.ResourceUtils
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration

class NativeCallStreamTest {
    private val upstreamId = "upstreamId"
    private val mapper = Global.objectMapper

    @Test
    fun `streaming response is equal to the original response`() {
        val responseFile = ResourceUtils.getFile("classpath:responses/get-by-number-response.json")
        val response = mapper.writeValueAsBytes(mapper.readValue(responseFile, JsonNode::class.java))
        val nativeCallResponse = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setId(1)
            .setSucceed(true)
            .setUpstreamId(upstreamId)
            .setPayload(ByteString.copyFrom(response))
            .build()
        val nativeCallMock = mock<NativeCall> {
            on { nativeCall(any()) } doReturn Flux.just(nativeCallResponse)
        }
        val nativeCallStream = NativeCallStream(nativeCallMock)
        val req = Mono.just(
            NativeCallRequest.newBuilder()
                .setChunkSize(1000)
                .build()
        )

        val result = nativeCallStream.nativeCall(req)
            .collectList()
            .block()!!
            .map { it.payload.toByteArray() }
            .reduce { acc, bytes -> acc.plus(bytes) }

        assertTrue(response.contentEquals(result))
    }

    @Test
    fun `streaming responses is correct`() {
        val response = "\"0x1126938\"".toByteArray()
        val nativeCallResponse = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setId(15)
            .setSucceed(true)
            .setUpstreamId(upstreamId)
            .setPayload(ByteString.copyFrom(response))
            .build()
        val nativeCallMock = mock<NativeCall> {
            on { nativeCall(any()) } doReturn Flux.just(nativeCallResponse)
        }
        val nativeCallStream = NativeCallStream(nativeCallMock)
        val req = Mono.just(
            NativeCallRequest.newBuilder()
                .setChunkSize(5)
                .build()
        )

        val chunkResponse: (Int) -> BlockchainOuterClass.NativeCallReplyItem.Builder = { id ->
            BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                .setId(id)
                .setChunked(true)
                .setSucceed(true)
                .setUpstreamId(upstreamId)
        }

        val result = nativeCallStream.nativeCall(req)

        StepVerifier.create(result)
            .expectNext(
                chunkResponse(15)
                    .setPayload(ByteString.copyFrom("\"0x11".toByteArray()))
                    .build()
            )
            .expectNext(
                chunkResponse(15)
                    .setPayload(ByteString.copyFrom("26938".toByteArray()))
                    .build()
            )
            .expectNext(
                chunkResponse(15)
                    .setFinalChunk(true)
                    .setPayload(ByteString.copyFrom("\"".toByteArray()))
                    .build()
            )
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    @Test
    fun `no streaming if response is too small`() {
        val response = "\"0x1\"".toByteArray()
        val nativeCallResponse = BlockchainOuterClass.NativeCallReplyItem.newBuilder()
            .setId(15)
            .setSucceed(true)
            .setUpstreamId(upstreamId)
            .setPayload(ByteString.copyFrom(response))
            .build()
        val nativeCallMock = mock<NativeCall> {
            on { nativeCall(any()) } doReturn Flux.just(nativeCallResponse)
        }
        val nativeCallStream = NativeCallStream(nativeCallMock)
        val req = Mono.just(
            NativeCallRequest.newBuilder()
                .setChunkSize(1000)
                .build()
        )

        val result = nativeCallStream.nativeCall(req)

        StepVerifier.create(result)
            .expectNext(
                nativeCallResponse
            )
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    @Test
    fun `sort responses by request id is correct`() {
        val response = "\"0x1\"".toByteArray()
        val response2 = "\"0x2\"".toByteArray()
        val response3 = "\"0x3\"".toByteArray()

        val nativeCallResponse: (Int, ByteArray) -> BlockchainOuterClass.NativeCallReplyItem = { id, resp ->
            BlockchainOuterClass.NativeCallReplyItem.newBuilder()
                .setId(id)
                .setChunked(true)
                .setSucceed(true)
                .setUpstreamId(upstreamId)
                .setPayload(ByteString.copyFrom(resp))
                .build()
        }
        val nativeCallMock = mock<NativeCall> {
            on { nativeCall(any()) } doReturn Flux.just(
                nativeCallResponse(1, response), nativeCallResponse(2, response2), nativeCallResponse(3, response3)
            ).flatMap {
                when (it.id) {
                    1 -> Mono.just(it).delayElement(Duration.ofMillis(200))
                    2 -> Mono.just(it).delayElement(Duration.ofMillis(100))
                    else -> Mono.just(it)
                }
            }
        }
        val nativeCallStream = NativeCallStream(nativeCallMock)
        val req = Mono.just(
            NativeCallRequest.newBuilder()
                .setSorted(true)
                .build()
        )

        val result = nativeCallStream.nativeCall(req)

        StepVerifier.create(result)
            .expectNextMatches { it.payload.toByteArray().contentEquals(response) }
            .expectNextMatches { it.payload.toByteArray().contentEquals(response2) }
            .expectNextMatches { it.payload.toByteArray().contentEquals(response3) }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }
}
