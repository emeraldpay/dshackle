package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk

class NotLaggingQuorumTest :
    ShouldSpec({

        should("Resolve if no lag") {
            val quorum = NotLaggingQuorum(1)
            val upstream = mockk<Upstream>()
            every { upstream.getLag() } returns 0
            quorum.record("foo".toByteArray(), null, upstream)

            quorum.isResolved() shouldBe true
            quorum.isFailed() shouldBe false
            quorum.getResult() shouldBe "foo".toByteArray()
        }

        should("Keep signature") {
            val quorum = NotLaggingQuorum(1)
            val upstream = mockk<Upstream>()
            every { upstream.getLag() } returns 0
            val signature = ResponseSigner.Signature("sig1".toByteArray(), "test", 100)

            quorum.record("foo".toByteArray(), signature, upstream)

            quorum.isResolved() shouldBe true
            quorum.isFailed() shouldBe false
            quorum.getResult() shouldBe "foo".toByteArray()

            quorum.getSignature() shouldBe signature
        }

        should("Resolve if ok lag") {
            val quorum = NotLaggingQuorum(1)
            val upstream = mockk<Upstream>()
            every { upstream.getLag() } returns 1
            val value = "foo".toByteArray()

            quorum.record(value, null, upstream)

            quorum.isResolved() shouldBe true
            quorum.isFailed() shouldBe false
            quorum.getResult() shouldBe value
        }

        should("Ignore if lags") {
            val quorum = NotLaggingQuorum(1)
            val upstream = mockk<Upstream>()
            every { upstream.getLag() } returns 2

            quorum.record("foo".toByteArray(), null, upstream)

            quorum.isResolved() shouldBe false
            // not failed because it can repeat
            quorum.isFailed() shouldBe false
        }

        should("Fail if no lag but an error response received") {
            val quorum = NotLaggingQuorum(1)
            val upstream = mockk<Upstream>()
            every { upstream.getLag() } returns 0

            quorum.record(JsonRpcException(-100, "test error"), null, upstream)

            quorum.isResolved() shouldBe false
            quorum.isFailed() shouldBe true
        }
    })
