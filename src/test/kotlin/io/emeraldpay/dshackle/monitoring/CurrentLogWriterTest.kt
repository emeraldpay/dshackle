package io.emeraldpay.dshackle.monitoring

import io.emeraldpay.dshackle.config.LogTargetConfig
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.should
import io.kotest.matchers.types.beInstanceOf
import java.time.Duration

class CurrentLogWriterTest : ShouldSpec({

    val serializer = { it: String -> it.toByteArray() }
    class TestImpl(fileOptions: FileOptions? = null) : CurrentLogWriter<String>(Category.REQUEST, serializer, fileOptions)

    should("Create file logger") {
        val current = TestImpl(CurrentLogWriter.FileOptions(Duration.ofSeconds(1), Duration.ofSeconds(1), 100))
        val config = LogTargetConfig.File("test.txt")
        current.setup(config)

        current.logWriter should beInstanceOf<FileLogWriter<String>>()
    }

    should("Create socket logger") {
        val current = TestImpl()
        val config = LogTargetConfig.Socket("localhost", 1234)
        current.setup(config)

        current.logWriter should beInstanceOf<SocketLogWriter<String>>()
    }
})
