package io.emeraldpay.dshackle.monitoring

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.optional.bePresent
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNot
import reactor.test.StepVerifier
import java.lang.RuntimeException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration

class BufferingLogWriterTest : ShouldSpec({

    val defaultSerializer = { x: String -> x.toByteArray() }
    class TempImpl(
        queueLimit: Int,
        serializer: (String) -> ByteArray? = defaultSerializer,
        encoding: LogEncoding = LogEncodingNewLine(),
    ) : BufferingLogWriter<String>(
        serializer = serializer,
        encoding = encoding,
        queueLimit = queueLimit
    ) {
        override fun start() {}
        override fun isRunning(): Boolean = true
    }

    should("Accept and produce event") {
        val writer = TempImpl(10)

        writer.submit("test")

        StepVerifier.create(writer.readFromQueue())
            .expectNext("test")
            .then { writer.stop() }
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Accept and produce and encoded event") {
        val writer = TempImpl(10)

        writer.submit("test")
        writer.submit("test2")

        StepVerifier.create(writer.readEncodedFromQueue().map { StandardCharsets.UTF_8.decode(it).toString() })
            .expectNext("test\n")
            .expectNext("test2\n")
            .then { writer.stop() }
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Ignore serializer errors") {
        val writer = TempImpl(10, serializer = {
            if (it == "fail") {
                throw RuntimeException()
            }
            it.toByteArray()
        })

        val fail = writer.toByteBuffer("fail")
        fail shouldNot bePresent()

        val test = writer.toByteBuffer("test")
        test should bePresent()
        StandardCharsets.UTF_8.decode(test.get()).toString() shouldBe "test\n"
    }

    should("Produce ignoring serializer errors") {
        val writer = TempImpl(10, serializer = {
            if (it == "fail") {
                throw RuntimeException()
            }
            it.toByteArray()
        })

        writer.submit("fail")
        writer.submit("test")

        StepVerifier.create(
            writer.readEncodedFromQueue()
                .map { StandardCharsets.UTF_8.decode(it).toString() }
        )
            .expectNext("test\n")
            .then { writer.stop() }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }

    should("Ignore encoding errors") {
        val writer = TempImpl(
            10,
            encoding = object : LogEncoding {
                override fun write(bytes: ByteArray): ByteBuffer {
                    if ("fail" == String(bytes)) {
                        throw RuntimeException()
                    }
                    return ByteBuffer.wrap(bytes)
                }
            }
        )

        val fail = writer.toByteBuffer("fail")
        fail shouldNot bePresent()

        val test = writer.toByteBuffer("test")
        test should bePresent()
        StandardCharsets.UTF_8.decode(test.get()).toString() shouldBe "test"
    }

    should("Produce ignoring encoder errors") {
        val writer = TempImpl(
            10,
            encoding = object : LogEncoding {
                override fun write(bytes: ByteArray): ByteBuffer {
                    if ("fail" == String(bytes)) {
                        throw RuntimeException()
                    }
                    return ByteBuffer.wrap(bytes)
                }
            }
        )

        writer.submit("fail")
        writer.submit("test")

        StepVerifier.create(
            writer.readEncodedFromQueue()
                .map { StandardCharsets.UTF_8.decode(it).toString() }
        )
            .expectNext("test")
            .then { writer.stop() }
            .expectComplete()
            .verify(Duration.ofSeconds(3))
    }
})
