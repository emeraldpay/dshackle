package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.mockk

class NonEmptyQuorumTest :
    ShouldSpec({

        should("Fail more errors than tries") {
            val quorum = NonEmptyQuorum(3)
            val upstream1 = mockk<Upstream>()
            val upstream2 = mockk<Upstream>()
            val upstream3 = mockk<Upstream>()

            quorum.record(JsonRpcException(1, "Internal"), null, upstream1)
            quorum.isResolved() shouldBe false
            quorum.isFailed() shouldBe false

            quorum.record(JsonRpcException(1, "Internal"), null, upstream2)
            quorum.isResolved() shouldBe false
            quorum.isFailed() shouldBe false

            quorum.record(JsonRpcException(1, "Internal"), null, upstream3)
            quorum.isResolved() shouldBe false
            quorum.isFailed() shouldBe true
        }

        should("Result with the first if no errors") {
            val quorum = NonEmptyQuorum(3)
            val upstream1 = mockk<Upstream>()

            quorum.record("\"0x11\"".toByteArray(), null, upstream1)
            quorum.isResolved() shouldBe true
            quorum.isFailed() shouldBe false

            quorum.getResult() shouldBe "\"0x11\"".toByteArray()
        }

        should("Result with the first after error") {
            val quorum = NonEmptyQuorum(3)
            val upstream1 = mockk<Upstream>()
            val upstream2 = mockk<Upstream>()

            quorum.record(JsonRpcException(1, "Internal"), null, upstream1)
            quorum.isResolved() shouldBe false
            quorum.isFailed() shouldBe false

            quorum.record("\"0x11\"".toByteArray(), null, upstream2)
            quorum.isResolved() shouldBe true
            quorum.isFailed() shouldBe false

            quorum.getResult() shouldBe "\"0x11\"".toByteArray()
        }

        should("Result with the a non-null value") {
            val quorum = NonEmptyQuorum(3)
            val upstream1 = mockk<Upstream>()
            val upstream2 = mockk<Upstream>()

            quorum.record("null".toByteArray(), null, upstream1)
            quorum.isResolved() shouldBe false
            quorum.isFailed() shouldBe false

            quorum.record("\"0x11\"".toByteArray(), null, upstream2)
            quorum.isResolved() shouldBe true
            quorum.isFailed() shouldBe false

            quorum.getResult() shouldBe "\"0x11\"".toByteArray()
        }
    })
