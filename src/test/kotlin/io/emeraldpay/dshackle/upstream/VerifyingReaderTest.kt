package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.etherjar.rpc.RpcException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.contain
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class VerifyingReaderTest : ShouldSpec({

    should("Return empty for allowed method") {
        val methods = mockk<CallMethods>()
        every { methods.isAvailable("test_foo") } returns true
        val reader = VerifyingReader(AtomicReference(methods))

        val act = reader.read(DshackleRequest("test_foo", emptyList()))
            .block(Duration.ofSeconds(1))

        act shouldBe null
        verify(exactly = 1) { methods.isAvailable(any()) }
    }

    should("Return error for allowed method") {
        val methods = mockk<CallMethods>()
        every { methods.isAvailable("test_foo") } returns false
        val reader = VerifyingReader(AtomicReference(methods))

        val t = shouldThrow<RpcException> {
            reader.read(DshackleRequest("test_foo", emptyList()))
                .block(Duration.ofSeconds(1))
        }

        t shouldNotBe null
        t.message!! should contain("Unsupported method: test_foo")

        verify(exactly = 1) { methods.isAvailable(any()) }
    }
})
