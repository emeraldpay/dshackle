package io.emeraldpay.dshackle.config

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf

class LogTargetConfigReaderTest : ShouldSpec({

    val defaultFile = LogTargetConfig.File(
        filename = "./test.log"
    )

    context("File Config") {

        should("Read Basic") {
            val yaml = """
                type: file
            """.trimIndent()
            val reader = LogTargetConfigReader(defaultFile)

            val act = reader.read(reader.readNode(yaml))
            act shouldBe instanceOf<LogTargetConfig.File>()
            (act as LogTargetConfig.File).also {
                it.filename shouldBe "./test.log"
            }
        }

        should("Read with file path") {
            val yaml = """
                type: file
                filename: /var/log/hello.log
            """.trimIndent()
            val reader = LogTargetConfigReader(defaultFile)

            val act = reader.read(reader.readNode(yaml))
            act shouldBe instanceOf<LogTargetConfig.File>()
            (act as LogTargetConfig.File).also {
                it.filename shouldBe "/var/log/hello.log"
            }
        }
    }

    context("Socket Config") {
        should("Read Basic") {
            val yaml = """
                type: socket
                port: 9000
            """.trimIndent()
            val reader = LogTargetConfigReader(defaultFile)

            val act = reader.read(reader.readNode(yaml))
            act shouldBe instanceOf<LogTargetConfig.Socket>()
            (act as LogTargetConfig.Socket).also {
                it.host shouldBe "127.0.0.1"
                it.port shouldBe 9000
                it.encoding shouldBe LogTargetConfig.Encoding.SIZE_PREFIX
            }
        }

        should("Read custom host") {
            val yaml = """
                type: socket
                port: 9000
                host: 10.0.5.20
            """.trimIndent()
            val reader = LogTargetConfigReader(defaultFile)

            val act = reader.read(reader.readNode(yaml))
            act shouldBe instanceOf<LogTargetConfig.Socket>()
            (act as LogTargetConfig.Socket).also {
                it.host shouldBe "10.0.5.20"
            }
        }

        should("Read NL encoding") {
            val yaml = """
                type: socket
                port: 9000
                encoding: new-line
            """.trimIndent()
            val reader = LogTargetConfigReader(defaultFile)

            val act = reader.read(reader.readNode(yaml))
            act shouldBe instanceOf<LogTargetConfig.Socket>()
            (act as LogTargetConfig.Socket).also {
                it.encoding shouldBe LogTargetConfig.Encoding.NEW_LINE
            }
        }

        should("Read Prefix encoding") {
            val yaml = """
                type: socket
                port: 9000
                encoding: length-prefix
            """.trimIndent()
            val reader = LogTargetConfigReader(defaultFile)

            val act = reader.read(reader.readNode(yaml))
            act shouldBe instanceOf<LogTargetConfig.Socket>()
            (act as LogTargetConfig.Socket).also {
                it.encoding shouldBe LogTargetConfig.Encoding.SIZE_PREFIX
            }
        }
    }
})
