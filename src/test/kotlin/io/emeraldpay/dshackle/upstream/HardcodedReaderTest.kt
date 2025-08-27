package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class HardcodedReaderTest :
    ShouldSpec({

        should("Return hardcoded as is") {

            val delegate = mockk<CallMethods>()
            every { delegate.isHardcoded("test_foo") } returns true
            every { delegate.executeHardcoded("test_foo") } returns "Test".toByteArray()

            val reader = HardcodedReader(AtomicReference(delegate))
            val act =
                reader
                    .read(DshackleRequest("test_foo", emptyList()))
                    .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
            act.resultAsRawString shouldBe "Test"
        }

        should("Return nothing for not hardcoded") {

            val delegate = mockk<CallMethods>()
            every { delegate.isHardcoded("test_foo") } returns false

            val reader = HardcodedReader(AtomicReference(delegate))
            val act =
                reader
                    .read(DshackleRequest("test_foo", emptyList()))
                    .block(Duration.ofSeconds(1))

            act shouldBe null
        }

        should("Use new methods if ref changes") {

            val ref = AtomicReference<CallMethods>(DefaultEthereumMethods(Chain.ETHEREUM))

            val reader = HardcodedReader(ref)

            val initial =
                reader
                    .read(DshackleRequest("test_foo", emptyList()))
                    .block(Duration.ofSeconds(1))
            initial shouldBe null

            val delegate = mockk<CallMethods>()
            every { delegate.isHardcoded("test_foo") } returns true
            every { delegate.executeHardcoded("test_foo") } returns "Test".toByteArray()
            ref.set(delegate)

            val act =
                reader
                    .read(DshackleRequest("test_foo", emptyList()))
                    .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hasResult shouldBe true
            act.resultAsRawString shouldBe "Test"
        }
    })
