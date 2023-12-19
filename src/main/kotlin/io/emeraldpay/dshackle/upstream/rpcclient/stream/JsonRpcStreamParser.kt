package io.emeraldpay.dshackle.upstream.rpcclient.stream

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.ResponseRpcParser
import io.emeraldpay.etherjar.rpc.RpcResponseError
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.ByteBufFlux
import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class JsonRpcStreamParser(
    private val firstChunkMaxSize: Int = 8192,
) {
    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcStreamParser::class.java)

        private val jsonFactory = JsonFactory()
        private val responseRpcParser = ResponseRpcParser()

        private const val ARRAY_OPEN_BRACKET: Byte = '['.code.toByte()
        private const val ARRAY_CLOSE_BRACKET: Byte = ']'.code.toByte()
        private const val OBJECT_OPEN_BRACKET: Byte = '{'.code.toByte()
        private const val OBJECT_CLOSE_BRACKET: Byte = '}'.code.toByte()
        private const val BACKSLASH: Byte = '\\'.code.toByte()
        private const val QUOTE: Byte = '"'.code.toByte()
    }

    fun streamParse(statusCode: Int, response: Flux<ByteArray>): Mono<out Response> {
        val firstPartSize = AtomicInteger()
        return response.bufferUntil {
            if (firstPartSize.get() > firstChunkMaxSize) {
                true
            } else {
                firstPartSize.addAndGet(it.size)
                firstPartSize.get() > firstChunkMaxSize // accumulate bytes until chunk is full
            }
        }.map {
            it.reduce { acc, bytes -> acc.plus(bytes) }
        }.switchOnFirst({ first, responseStream ->
            if (first.get() == null || statusCode != 200) {
                aggregateResponse(responseStream, statusCode)
            } else {
                val whatCount = AtomicReference<Count>()
                val endStream = AtomicBoolean(false)

                val firstBytes = first.get()!!

                val firstPart: SingleResponse? = parseFirstPart(firstBytes, endStream, whatCount)

                if (firstPart == null) {
                    aggregateResponse(responseStream, statusCode)
                } else {
                    processSingleResponse(firstPart, responseStream, endStream, whatCount)
                }
            }
        }, false,)
            .single()
            .onErrorResume {
                Mono.just(
                    SingleResponse(
                        null,
                        JsonRpcError(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, it.message ?: "Internal error"),
                    ),
                )
            }
    }

    private fun processSingleResponse(
        response: SingleResponse,
        responseStream: Flux<ByteArray>,
        endStream: AtomicBoolean,
        whatCount: AtomicReference<Count>,
    ): Mono<out Response> {
        if (response.noResponse()) {
            throw IllegalStateException("Invalid JSON structure")
        } else {
            if (response.hasError()) {
                return Mono.just(response)
            } else {
                return if (endStream.get()) {
                    Mono.just(SingleResponse(response.result, null))
                } else {
                    Mono.just(
                        StreamResponse(
                            streamParts(
                                response.result!!,
                                responseStream,
                                endStream,
                                whatCount,
                            ),
                        ),
                    )
                }
            }
        }
    }

    private fun aggregateResponse(response: Flux<ByteArray>, statusCode: Int): Mono<AggregateResponse> {
        return ByteBufFlux.fromInbound(response).aggregate().asByteArray()
            .map { AggregateResponse(it, statusCode) }
    }

    private fun streamParts(
        firstBytes: ByteArray,
        responseStream: Flux<ByteArray>,
        endStream: AtomicBoolean,
        whatCount: AtomicReference<Count>,
    ): Flux<Chunk> {
        return Flux.concat(
            Mono.just(Chunk(firstBytes, false)),
            responseStream.skip(1)
                .filter { !endStream.get() }
                .map { bytes ->
                    val whatCountValue = whatCount.get()
                    for (i in bytes.indices) {
                        when (whatCountValue) {
                            is CountObjectBrackets -> {
                                countBrackets(bytes[i], whatCountValue.count, OBJECT_OPEN_BRACKET, OBJECT_CLOSE_BRACKET)
                            }

                            is CountArrayBrackets -> {
                                countBrackets(bytes[i], whatCountValue.count, ARRAY_OPEN_BRACKET, ARRAY_CLOSE_BRACKET)
                            }

                            is CountSlashes -> {
                                countQuotesAndSlashes(bytes[i], whatCountValue)
                            }
                        }
                        if (whatCountValue.isFinished()) {
                            endStream.set(true)
                            return@map Chunk(Arrays.copyOfRange(bytes, 0, i + 1), true)
                        }
                    }
                    Chunk(bytes, false)
                },
            Mono.just(endStream)
                .flatMap {
                    if (!it.get()) {
                        Mono.just(Chunk(ByteArray(0), true))
                    } else {
                        Mono.empty()
                    }
                },
        )
    }

    private fun parseFirstPart(
        firstBytes: ByteArray,
        endStream: AtomicBoolean,
        whatCount: AtomicReference<Count>,
    ): SingleResponse? {
        try {
            jsonFactory.createParser(firstBytes).use { parser ->
                while (true) {
                    parser.nextToken()
                    if (firstBytes.size == parser.currentLocation.byteOffset.toInt()) {
                        break
                    }
                    if (parser.currentName != null) {
                        if (parser.currentName == "result") {
                            val token = parser.nextToken()
                            val tokenStart = parser.tokenLocation.byteOffset.toInt()
                            return if (token.isScalarValue) {
                                val count = CountSlashes(AtomicInteger(1))
                                whatCount.set(count)
                                SingleResponse(
                                    processScalarValue(parser, tokenStart, firstBytes, count, endStream),
                                    null,
                                )
                            } else {
                                when (token) {
                                    JsonToken.START_OBJECT -> {
                                        val count = CountObjectBrackets(AtomicInteger(1))
                                        whatCount.set(count)
                                        SingleResponse(
                                            processAndCountBrackets(
                                                tokenStart,
                                                firstBytes,
                                                count.count,
                                                endStream,
                                                OBJECT_OPEN_BRACKET,
                                                OBJECT_CLOSE_BRACKET,
                                            ),
                                            null,
                                        )
                                    }

                                    JsonToken.START_ARRAY -> {
                                        val count = CountArrayBrackets(AtomicInteger(1))
                                        whatCount.set(count)
                                        SingleResponse(
                                            processAndCountBrackets(
                                                tokenStart,
                                                firstBytes,
                                                count.count,
                                                endStream,
                                                ARRAY_OPEN_BRACKET,
                                                ARRAY_CLOSE_BRACKET,
                                            ),
                                            null,
                                        )
                                    }

                                    else -> {
                                        throw IllegalStateException("'result' not an object nor array'")
                                    }
                                }
                            }
                        } else if (parser.currentName == "error") {
                            return SingleResponse(null, responseRpcParser.readError(parser))
                        }
                    }
                }
                return null
            }
        } catch (e: Exception) {
            log.warn("Streaming parsing exception: {}", e.message)
            return null
        }
    }

    private fun processAndCountBrackets(
        tokenStart: Int,
        bytes: ByteArray,
        brackets: AtomicInteger,
        endStream: AtomicBoolean,
        openBracket: Byte,
        closeBracket: Byte,
    ): ByteArray {
        for (i in tokenStart + 1 until bytes.size) {
            countBrackets(bytes[i], brackets, openBracket, closeBracket)
            if (brackets.get() == 0) {
                endStream.set(true)
                return Arrays.copyOfRange(bytes, tokenStart, i + 1)
            }
        }
        return Arrays.copyOfRange(bytes, tokenStart, bytes.size)
    }

    private fun countBrackets(
        byte: Byte,
        brackets: AtomicInteger,
        openBracket: Byte,
        closeBracket: Byte,
    ) {
        if (byte == openBracket) {
            brackets.incrementAndGet()
        } else if (byte == closeBracket) {
            brackets.decrementAndGet()
        }
    }

    private fun countQuotesAndSlashes(
        byte: Byte,
        countSlashes: CountSlashes,
    ) {
        if (byte == BACKSLASH && !countSlashes.hasSlash()) {
            countSlashes.count.incrementAndGet()
        } else if (countSlashes.hasSlash()) {
            countSlashes.count.decrementAndGet()
        } else if (!countSlashes.hasSlash() && byte == QUOTE) {
            countSlashes.count.set(0)
        }
    }

    private fun processScalarValue(
        parser: JsonParser,
        tokenStart: Int,
        bytes: ByteArray,
        countSlashes: CountSlashes,
        endStream: AtomicBoolean,
    ): ByteArray {
        when (parser.currentToken) {
            JsonToken.VALUE_NULL -> {
                endStream.set(true)
                return "null".toByteArray()
            }
            JsonToken.VALUE_STRING -> {
                for (i in tokenStart + 1 until bytes.size) {
                    countQuotesAndSlashes(bytes[i], countSlashes)
                    if (countSlashes.isFinished()) {
                        endStream.set(true)
                        return Arrays.copyOfRange(bytes, tokenStart, i + 1)
                    }
                }
                return Arrays.copyOfRange(bytes, tokenStart, bytes.size)
            }
            else -> {
                endStream.set(true)
                return parser.text.toByteArray()
            }
        }
    }

    private abstract class Count(
        val count: AtomicInteger,
    ) {
        open fun isFinished(): Boolean = count.get() == 0
    }

    private class CountArrayBrackets(
        countBrackets: AtomicInteger,
    ) : Count(countBrackets)

    private class CountObjectBrackets(
        countBrackets: AtomicInteger,
    ) : Count(countBrackets)

    private class CountSlashes(
        countSlashes: AtomicInteger,
    ) : Count(countSlashes) {

        fun hasSlash() = count.get() == 2
    }
}
