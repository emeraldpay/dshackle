package io.emeraldpay.dshackle.monitoring

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import java.nio.charset.StandardCharsets

class LogEncodingNewLineTest : ShouldSpec({

    should("Write a line") {
        val encoding = LogEncodingNewLine()

        val act = encoding.write("Hello World!".toByteArray())

        StandardCharsets.UTF_8.decode(act).toString() shouldBe "Hello World!\n"
    }
})
