package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk

class BroadcastQuorumTest : ShouldSpec({

    val objectMapper = Global.objectMapper

    should("Resolve with the first after 3 retries") {
        val quorum = BroadcastQuorum(3)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()
        val upstream3 = mockk<Upstream>()

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream1)
        quorum.isResolved() shouldBe false

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream2)
        quorum.isResolved() shouldBe false

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream3)
        quorum.isResolved() shouldBe true

        val result = objectMapper.readValue(quorum.getResult(), String::class.java)
        result shouldBe "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"

        quorum.isFailed() shouldBe false
    }

    should("Resolve with the first ok") {
        val quorum = BroadcastQuorum(3)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()
        val upstream3 = mockk<Upstream>()

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream1)
        quorum.isResolved() shouldBe false

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream2)
        quorum.isResolved() shouldBe false

        quorum.record(JsonRpcException(1, "Nonce too low"), null, upstream3)
        quorum.isResolved() shouldBe true

        val result = objectMapper.readValue(quorum.getResult(), String::class.java)
        result shouldBe "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"

        quorum.isFailed() shouldBe false
    }

    should("Use first valid") {
        val quorum = BroadcastQuorum(3)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()
        val upstream3 = mockk<Upstream>()

        quorum.record(JsonRpcException(1, "Internal error"), null, upstream1)
        quorum.isResolved() shouldBe false

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream2)
        quorum.isResolved() shouldBe false

        quorum.record(JsonRpcException(1, "Nonce too low"), null, upstream3)
        quorum.isResolved() shouldBe true

        val result = objectMapper.readValue(quorum.getResult(), String::class.java)
        result shouldBe "0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c"

        quorum.isFailed() shouldBe false
    }

    should("Fail if all failed") {
        val quorum = BroadcastQuorum(3)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()
        val upstream3 = mockk<Upstream>()

        quorum.record(JsonRpcException(1, "Error 1"), null, upstream1)
        quorum.isResolved() shouldBe false

        quorum.record(JsonRpcException(1, "Error 2"), null, upstream2)
        quorum.isResolved() shouldBe false

        quorum.record(JsonRpcException(1, "Error 3"), null, upstream3)

        quorum.isResolved() shouldBe false
        quorum.getResult() shouldBe null
        quorum.isFailed() shouldBe true
        quorum.getError() shouldNotBe null
        quorum.getError()!!.message shouldBe "Error 3"
    }

    should("Just once for a single upstream") {
        val quorum = BroadcastQuorum(3)
        quorum.setTotalUpstreams(1)
        val upstream1 = mockk<Upstream>()

        quorum.record("\"0xeaa972c0d8d1ecd3e34fbbef6d34e06670e745c788bdba31c4234a1762f0378c\"".toByteArray(), null, upstream1)
        quorum.isResolved() shouldBe true
        quorum.isFailed() shouldBe false
    }
})
