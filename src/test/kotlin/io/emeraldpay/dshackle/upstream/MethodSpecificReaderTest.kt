package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Mono
import java.lang.IllegalStateException
import java.time.Duration

class MethodSpecificReaderTest : ShouldSpec({

    should("Call when method is registered") {
        val delegate = mockk<DshackleRpcReader>()

        val reader = MethodSpecificReader()
        reader.register("test_foo", delegate)

        every { delegate.read(any()) } returns Mono.just(DshackleResponse(1, "\"Test\"".toByteArray()))

        val act = reader.read(DshackleRequest("test_foo", emptyList()))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        act.hasResult shouldBe true
        act.resultAsProcessedString shouldBe "Test"
    }

    should("Return empty when method is not registered") {
        val delegate = mockk<DshackleRpcReader>()

        val reader = MethodSpecificReader()
        reader.register("test_foo1", delegate)

        every { delegate.read(any()) } returns Mono.error(IllegalStateException())

        val act = reader.read(DshackleRequest("test_foo2", emptyList()))
            .block(Duration.ofSeconds(1))

        act shouldBe null
    }

    should("Call when method is registered and the predicate is ok") {
        val delegate = mockk<DshackleRpcReader>()

        val reader = MethodSpecificReader()
        reader.register("test_foo", { it == listOf(1) }, delegate)

        every { delegate.read(any()) } returns Mono.just(DshackleResponse(1, "\"Test\"".toByteArray()))

        val act1 = reader.read(DshackleRequest("test_foo", emptyList()))
            .block(Duration.ofSeconds(1))

        act1 shouldBe null

        val act2 = reader.read(DshackleRequest("test_foo", listOf(0)))
            .block(Duration.ofSeconds(1))

        act2 shouldBe null

        val act3 = reader.read(DshackleRequest("test_foo", listOf(1)))
            .block(Duration.ofSeconds(1))

        act3 shouldNotBe null
    }
})
