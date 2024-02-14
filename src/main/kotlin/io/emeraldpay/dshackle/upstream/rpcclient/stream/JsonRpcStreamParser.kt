package io.emeraldpay.dshackle.upstream.rpcclient.stream

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.ResponseRpcParser
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
                                countBrackets(bytes[i], whatCountValue, OBJECT_OPEN_BRACKET, OBJECT_CLOSE_BRACKET)
                            }

                            is CountArrayBrackets -> {
                                countBrackets(bytes[i], whatCountValue, ARRAY_OPEN_BRACKET, ARRAY_CLOSE_BRACKET)
                            }

                            is CountQuotesAndSlashes -> {
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
                                val count = CountQuotesAndSlashes(AtomicInteger(1))
                                whatCount.set(count)
                                SingleResponse(
                                    processScalarValue(parser, tokenStart, firstBytes, count, endStream),
                                    null,
                                )
                            } else {
                                when (token) {
                                    JsonToken.START_OBJECT -> {
                                        val count = CountObjectBrackets(
                                            AtomicInteger(1),
                                            CountQuotesAndSlashes(AtomicInteger(0)),
                                        )
                                        whatCount.set(count)
                                        SingleResponse(
                                            processAndCountBrackets(
                                                tokenStart,
                                                firstBytes,
                                                count,
                                                endStream,
                                                OBJECT_OPEN_BRACKET,
                                                OBJECT_CLOSE_BRACKET,
                                            ),
                                            null,
                                        )
                                    }

                                    JsonToken.START_ARRAY -> {
                                        val count = CountArrayBrackets(
                                            AtomicInteger(1),
                                            CountQuotesAndSlashes(AtomicInteger(0)),
                                        )
                                        whatCount.set(count)
                                        SingleResponse(
                                            processAndCountBrackets(
                                                tokenStart,
                                                firstBytes,
                                                count,
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
        countBrackets: CountBrackets,
        endStream: AtomicBoolean,
        openBracket: Byte,
        closeBracket: Byte,
    ): ByteArray {
        for (i in tokenStart + 1 until bytes.size) {
            countBrackets(bytes[i], countBrackets, openBracket, closeBracket)
            if (countBrackets.isFinished()) {
                endStream.set(true)
                return Arrays.copyOfRange(bytes, tokenStart, i + 1)
            }
        }
        return Arrays.copyOfRange(bytes, tokenStart, bytes.size)
    }

    private fun countBrackets(
        byte: Byte,
        countBrackets: CountBrackets,
        openBracket: Byte,
        closeBracket: Byte,
    ) {
        // we encounter the quote and the start of a string value
        if (byte == QUOTE && countBrackets.isCountQuotesAndSlashesFinished()) {
            countBrackets.quotesIncrement()
            return
        }

        // we ignore all braces inside a string value until we get to the end of the value
        if (!countBrackets.isCountQuotesAndSlashesFinished()) {
            countQuotesAndSlashes(byte, countBrackets.countQuotesAndSlashes)
            return
        }

        if (byte == openBracket) {
            countBrackets.increment()
        } else if (byte == closeBracket) {
            countBrackets.decrement()
        }
    }

    private fun countQuotesAndSlashes(
        byte: Byte,
        countQuotesAndSlashes: CountQuotesAndSlashes,
    ) {
        if (byte == BACKSLASH && !countQuotesAndSlashes.hasSlash()) {
            countQuotesAndSlashes.increment()
        } else if (countQuotesAndSlashes.hasSlash()) {
            countQuotesAndSlashes.decrement()
        } else if (!countQuotesAndSlashes.hasSlash() && byte == QUOTE) {
            countQuotesAndSlashes.reset()
        }
    }

    private fun processScalarValue(
        parser: JsonParser,
        tokenStart: Int,
        bytes: ByteArray,
        countQuotesAndSlashes: CountQuotesAndSlashes,
        endStream: AtomicBoolean,
    ): ByteArray {
        when (parser.currentToken) {
            JsonToken.VALUE_NULL -> {
                endStream.set(true)
                return "null".toByteArray()
            }
            JsonToken.VALUE_STRING -> {
                for (i in tokenStart + 1 until bytes.size) {
                    countQuotesAndSlashes(bytes[i], countQuotesAndSlashes)
                    if (countQuotesAndSlashes.isFinished()) {
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
        protected val count: AtomicInteger,
    ) {
        open fun isFinished(): Boolean = count.get() == 0

        fun increment() {
            count.incrementAndGet()
        }

        fun decrement() {
            count.decrementAndGet()
        }

        fun reset() {
            count.set(0)
        }
    }

    private open class CountBrackets(
        countBrackets: AtomicInteger,
        val countQuotesAndSlashes: CountQuotesAndSlashes,
    ) : Count(countBrackets) {
        fun quotesIncrement() {
            countQuotesAndSlashes.increment()
        }

        fun isCountQuotesAndSlashesFinished() = countQuotesAndSlashes.isFinished()
    }

    private class CountArrayBrackets(
        countBrackets: AtomicInteger,
        countQuotesAndSlashes: CountQuotesAndSlashes,
    ) : CountBrackets(countBrackets, countQuotesAndSlashes)

    private class CountObjectBrackets(
        countBrackets: AtomicInteger,
        countQuotesAndSlashes: CountQuotesAndSlashes,
    ) : CountBrackets(countBrackets, countQuotesAndSlashes)

    private class CountQuotesAndSlashes(
        countSlashes: AtomicInteger,
    ) : Count(countSlashes) {

        fun hasSlash() = count.get() == 2
    }
}
