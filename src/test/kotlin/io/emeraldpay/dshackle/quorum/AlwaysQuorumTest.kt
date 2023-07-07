package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk

class AlwaysQuorumTest : ShouldSpec({

    should("Fail if error received") {
        val quorum = AlwaysQuorum()
        val upstream = mockk<Upstream>()

        quorum.record(JsonRpcException(1, "test"), null, upstream)

        quorum.isFailed() shouldBe true
        quorum.isResolved() shouldBe false
        quorum.getError() shouldNotBe null
        quorum.getError()?.message shouldBe "test"
    }

    should("Resolve if result received") {
        val quorum = AlwaysQuorum()
        val upstream = mockk<Upstream>()

        quorum.record("123".toByteArray(), ResponseSigner.Signature("sig1".toByteArray(), "test", 100), upstream)

        quorum.isResolved() shouldBe true
        quorum.getResult() shouldBe "123".toByteArray()
        quorum.getSignature() shouldBe ResponseSigner.Signature("sig1".toByteArray(), "test", 100)
        quorum.isFailed() shouldBe false
    }
})
