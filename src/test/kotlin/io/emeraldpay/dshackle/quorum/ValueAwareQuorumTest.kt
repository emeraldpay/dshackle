package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe

class ValueAwareQuorumTest :
    ShouldSpec({

        should("Extract null") {
            val quorum = ValueAwareQuorumImpl()

            quorum.extractValue("null".toByteArray(), Any::class.java) shouldBe null
        }

        should("Extract string") {
            val quorum = ValueAwareQuorumImpl()

            quorum.extractValue("\"foo\"".toByteArray(), Any::class.java) shouldBe "foo"
        }

        should("Extract number") {
            val quorum = ValueAwareQuorumImpl()

            quorum.extractValue("100".toByteArray(), Any::class.java) shouldBe 100
        }

        should("Extract map") {
            val quorum = ValueAwareQuorumImpl()

            quorum.extractValue("{\"foo\": 1}".toByteArray(), Any::class.java) shouldBe mapOf(Pair("foo", 1))
        }
    }) {
    class ValueAwareQuorumImpl : ValueAwareQuorum<Any>(Any::class.java) {
        override fun recordValue(
            response: ByteArray,
            responseValue: Any?,
            signature: ResponseSigner.Signature?,
            upstream: Upstream,
        ) {
        }

        override fun recordError(
            response: ByteArray?,
            errorMessage: String?,
            signature: ResponseSigner.Signature?,
            upstream: Upstream,
        ) {
        }

        override fun init(head: Head) {
        }

        override fun setTotalUpstreams(total: Int) {
        }

        override fun isResolved(): Boolean = false

        override fun isFailed(): Boolean = false

        override fun getSignature(): ResponseSigner.Signature? = null

        override fun getResult(): ByteArray? = null
    }
}
