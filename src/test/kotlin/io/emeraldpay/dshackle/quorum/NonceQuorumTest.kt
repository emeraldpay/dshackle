package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.mockk

class NonceQuorumTest : ShouldSpec({

    val objectMapper = Global.objectMapper

    should("Get max value") {
        val quorum = NonceQuorum(3)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()
        val upstream3 = mockk<Upstream>()

        quorum.record("\"0x10\"".toByteArray(), null, upstream1)
        quorum.isResolved() shouldBe false
        quorum.record("\"0x11\"".toByteArray(), null, upstream2)
        quorum.isResolved() shouldBe false
        quorum.record("\"0x10\"".toByteArray(), null, upstream3)

        quorum.isResolved() shouldBe true
        objectMapper.readValue(quorum.getResult(), String::class.java) shouldBe "0x11"
        quorum.isFailed() shouldBe false
    }

    should("Request twice for two upstream") {
        val quorum = NonceQuorum(3)
        quorum.setTotalUpstreams(2)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()

        quorum.record("\"0x10\"".toByteArray(), null, upstream1)
        quorum.isResolved() shouldBe false
        quorum.record("\"0x11\"".toByteArray(), null, upstream2)

        quorum.isResolved() shouldBe true
        objectMapper.readValue(quorum.getResult(), String::class.java) shouldBe "0x11"
        quorum.isFailed() shouldBe false
    }

    should("Ignore error") {
        val quorum = NonceQuorum(3)
        val upstream1 = mockk<Upstream>()
        val upstream2 = mockk<Upstream>()
        val upstream3 = mockk<Upstream>()

        quorum.record(JsonRpcException(1, "Internal"), null, upstream3)
        quorum.isResolved() shouldBe false
        quorum.record("\"0x10\"".toByteArray(), null, upstream1)
        quorum.isResolved() shouldBe false
        quorum.record("\"0x11\"".toByteArray(), null, upstream2)
        quorum.isResolved() shouldBe false
        quorum.record("\"0x10\"".toByteArray(), null, upstream3)

        quorum.isResolved() shouldBe true
        objectMapper.readValue(quorum.getResult(), String::class.java) shouldBe "0x11"
        quorum.isFailed() shouldBe false
    }

    should("Fail if all failed") {
        val quorum = NonceQuorum(3)
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
})
