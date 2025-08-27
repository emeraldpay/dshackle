package io.emeraldpay.dshackle.monitoring

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import java.lang.RuntimeException
import java.nio.ByteBuffer

class BufferingLogWriterTest :
    ShouldSpec({

        class TempImpl(
            queueLimit: Int,
            serializer: LogSerializer<String> = MonitoringTestCommons.defaultSerializer,
            encoding: LogEncoding = LogEncodingNewLine(),
        ) : BufferingLogWriter<String>(
                serializer = serializer,
                encoding = encoding,
                queueLimit = queueLimit,
            ) {
            override fun start() {}

            override fun isRunning(): Boolean = true
        }

        should("Accept and produce event") {
            val writer = TempImpl(10)

            writer.submit("test")

            val next = writer.next(10)

            next shouldHaveSize 1
            next[0] shouldBe "test"
        }

        should("Accept and produce and encoded event") {
            val writer = TempImpl(10)

            writer.submit("test")
            writer.submit("test2")

            val next = writer.next(10)
            next shouldHaveSize 2
            MonitoringTestCommons.bufferToString(writer.encode(next[0])!!) shouldBe "test\n"
            MonitoringTestCommons.bufferToString(writer.encode(next[1])!!) shouldBe "test2\n"
        }

        should("Ignore serializer errors") {
            val writer = TempImpl(10, MonitoringTestCommons.failSerializer)

            val fail = writer.encode("fail")
            fail shouldBe null

            val test = writer.encode("test")
            test shouldNotBe null
            MonitoringTestCommons.bufferToString(test!!) shouldBe "test\n"
        }

        should("Produce ignoring serializer errors") {
            val writer = TempImpl(10, serializer = MonitoringTestCommons.failSerializer)

            writer.submit("fail")
            writer.submit("test")

            val next = writer.next(10)
            writer.encode(next[0]) shouldBe null
            MonitoringTestCommons.bufferToString(writer.encode(next[1])!!) shouldBe "test\n"
        }

        should("Ignore encoding errors") {
            val writer =
                TempImpl(
                    10,
                    encoding =
                        object : LogEncoding {
                            override fun write(bytes: ByteBuffer): ByteBuffer {
                                if ("fail" == String(bytes.array())) {
                                    throw RuntimeException()
                                }
                                return bytes
                            }
                        },
                )

            val fail = writer.encode("fail")
            fail shouldBe null

            val test = writer.encode("test")
            test shouldNotBe null
            MonitoringTestCommons.bufferToString(test!!) shouldBe "test"
        }

        should("Produce ignoring encoder errors") {
            val writer =
                TempImpl(
                    10,
                    encoding =
                        object : LogEncoding {
                            override fun write(bytes: ByteBuffer): ByteBuffer {
                                if ("fail" == String(bytes.array())) {
                                    throw RuntimeException()
                                }
                                return bytes
                            }
                        },
                )

            writer.submit("fail")
            writer.submit("test")

            val next = writer.next(10)
            writer.encode(next[0]) shouldBe null
            MonitoringTestCommons.bufferToString(writer.encode(next[1])!!) shouldBe "test"
        }

        should("Return unprocessed events") {
            val writer = TempImpl(10)

            writer.submit("test-1")
            writer.submit("test-2")
            writer.submit("test-3")
            writer.submit("test-4")
            writer.submit("test-5")

            val next = writer.next(10)

            next shouldHaveSize 5
            writer.returnBack(2, next)

            val next2 = writer.next(10)
            next2 shouldHaveSize 3
            MonitoringTestCommons.bufferToString(writer.encode(next2[0])!!) shouldBe "test-3\n"
            MonitoringTestCommons.bufferToString(writer.encode(next2[1])!!) shouldBe "test-4\n"
            MonitoringTestCommons.bufferToString(writer.encode(next2[2])!!) shouldBe "test-5\n"
        }
    })
