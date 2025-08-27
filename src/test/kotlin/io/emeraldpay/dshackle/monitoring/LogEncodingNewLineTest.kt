package io.emeraldpay.dshackle.monitoring

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer

class LogEncodingNewLineTest :
    ShouldSpec({

        should("Write a line") {
            val encoding = LogEncodingNewLine()

            val act = encoding.write(ByteBuffer.wrap("Hello World!".toByteArray()))

            MonitoringTestCommons.bufferToString(act) shouldBe "Hello World!\n"
        }
    })
