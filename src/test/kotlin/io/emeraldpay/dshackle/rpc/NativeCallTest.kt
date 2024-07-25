package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeCallReplyItem
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeCallRequest
import io.emeraldpay.dshackle.config.MainConfig
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.UNKNOWN_CLIENT_VERSION
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.emeraldpay.dshackle.upstream.stream.Chunk
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration

class NativeCallTest {

    @Test
    fun `nativeCall always returns item with response upstreamId`() {
        val request = Mono.just(NativeCallRequest.newBuilder().build())
        val nativeCall = spy(
            NativeCall(
                mock<MultistreamHolder>(),
                mock<ResponseSigner>(),
                MainConfig(),
                mock<Tracer>(),
            ),
        ) {
            on { nativeCallResult(request) } doReturn Flux.just(
                NativeCall.CallResult.ok(1, null, "0x1".toByteArray(), null, listOf(Upstream.UpstreamSettingsData("id")), null),
            )
        }

        StepVerifier.create(nativeCall.nativeCall(request))
            .expectNext(
                NativeCallReplyItem.newBuilder()
                    .setUpstreamId("id")
                    .setUpstreamNodeVersion(UNKNOWN_CLIENT_VERSION)
                    .setId(1)
                    .setSucceed(true)
                    .setPayload(ByteString.copyFrom("0x1".toByteArray()))
                    .build(),
            )
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    @Test
    fun `nativeCall always returns error item with response upstreamId`() {
        val request = Mono.just(NativeCallRequest.newBuilder().build())
        val nativeCall = spy(
            NativeCall(
                mock<MultistreamHolder>(),
                mock<ResponseSigner>(),
                MainConfig(),
                mock<Tracer>(),
            ),
        ) {
            on { nativeCallResult(request) } doReturn Flux.just(
                NativeCall.CallResult(1, null, null, NativeCall.CallError(50001, "message", null, null, listOf(Upstream.UpstreamSettingsData("upId"))), null, null),
            )
        }

        StepVerifier.create(nativeCall.nativeCall(request))
            .expectNext(
                NativeCallReplyItem.newBuilder()
                    .setUpstreamId("upId")
                    .setId(1)
                    .setUpstreamNodeVersion(UNKNOWN_CLIENT_VERSION)
                    .setSucceed(false)
                    .setErrorMessage("message")
                    .setItemErrorCode(50001)
                    .build(),
            )
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    @Test
    fun `nativeCall always returns item with response upstreamId in the first chunk only`() {
        val request = Mono.just(NativeCallRequest.newBuilder().build())
        val chunks = Flux.just(
            Chunk("0x1".toByteArray(), false),
            Chunk("111".toByteArray(), false),
            Chunk("222".toByteArray(), true),
        )
        val nativeCall = spy(
            NativeCall(
                mock<MultistreamHolder>(),
                mock<ResponseSigner>(),
                MainConfig(),
                mock<Tracer>(),
            ),
        ) {
            on { nativeCallResult(request) } doReturn Flux.just(
                NativeCall.CallResult.ok(1, null, "".toByteArray(), null, listOf(Upstream.UpstreamSettingsData("upId")), null, chunks),
            )
        }

        StepVerifier.create(nativeCall.nativeCall(request))
            .expectNext(
                NativeCallReplyItem.newBuilder()
                    .setUpstreamId("upId")
                    .setId(1)
                    .setUpstreamNodeVersion(UNKNOWN_CLIENT_VERSION)
                    .setChunked(true)
                    .setSucceed(true)
                    .setPayload(ByteString.copyFrom("0x1".toByteArray()))
                    .build(),
            )
            .expectNext(
                NativeCallReplyItem.newBuilder()
                    .setId(1)
                    .setChunked(true)
                    .setSucceed(true)
                    .setPayload(ByteString.copyFrom("111".toByteArray()))
                    .build(),
            )
            .expectNext(
                NativeCallReplyItem.newBuilder()
                    .setId(1)
                    .setChunked(true)
                    .setFinalChunk(true)
                    .setSucceed(true)
                    .setPayload(ByteString.copyFrom("222".toByteArray()))
                    .build(),
            )
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }
}
