package io.emeraldpay.dshackle.monitoring

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldEndWith
import io.kotest.matchers.string.shouldHaveLength
import io.kotest.matchers.string.shouldStartWith
import org.apache.commons.codec.binary.Hex

class LogEncodingPrefixTest : ShouldSpec({

    should("Encode single byte") {
        val act = LogEncodingPrefix().write("a".toByteArray())

        Hex.encodeHexString(act) shouldBe "0000000161"
    }

    should("Encode short string") {
        val act = LogEncodingPrefix().write("Hello World!".toByteArray())

        Hex.encodeHexString(act) shouldBe "0000000c48656c6c6f20576f726c6421"
    }

    should("Encode medium string") {
        val s = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
        val act = LogEncodingPrefix()
            .write(
                s.toByteArray()
            )

        val hex = Hex.encodeHexString(act)

        s shouldHaveLength 0x01bd
        hex shouldStartWith "000001bd"
        hex shouldEndWith Hex.encodeHexString(s.toByteArray())
        hex shouldHaveLength (4 + s.length) * 2
    }
})
