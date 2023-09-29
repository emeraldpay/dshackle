package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeCallReplyItem
import io.emeraldpay.api.proto.BlockchainOuterClass.NativeCallRequest
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.math.min

@Service
class NativeCallStream(
    private val nativeCall: NativeCall,
) {

    fun nativeCall(
        requestMono: Mono<NativeCallRequest>,
    ): Flux<NativeCallReplyItem> {
        return requestMono.flatMapMany { req ->
            nativeCall.nativeCall(Mono.just(req))
                .map { StreamNativeResult(it, req.chunkSize) }
                .transform {
                    if (!req.sorted || req.itemsList.size == 1) {
                        it
                    } else {
                        it.sort { o1, o2 -> o1.response.id - o2.response.id }
                    }
                }
        }.concatMap {
            val chunkSize = it.chunkSize
            val response = it.response
            if (chunkSize == 0 || response.payload.size() <= chunkSize || !response.succeed) {
                Mono.just(response)
            } else {
                Flux.fromIterable(chunks(response, chunkSize))
            }
        }
    }

    private fun chunks(response: NativeCallReplyItem, chunkSize: Int): List<NativeCallReplyItem> {
        val chunks = mutableListOf<ByteString>()
        val responseBytes = response.payload

        for (i in 0 until responseBytes.size() step+chunkSize) {
            chunks.add(responseBytes.substring(i, min(i + chunkSize, responseBytes.size())))
        }

        return chunks
            .mapIndexed { index, bytes ->
                NativeCallReplyItem.newBuilder()
                    .apply {
                        id = response.id
                        payload = bytes
                        succeed = true
                        upstreamId = response.upstreamId
                        chunked = true
                        finalChunk = index == chunks.size - 1
                        if (this.finalChunk && response.hasSignature()) {
                            signature = response.signature
                        }
                    }.build()
            }
    }

    private data class StreamNativeResult(
        val response: NativeCallReplyItem,
        val chunkSize: Int,
    )
}
