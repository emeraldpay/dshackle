package io.emeraldpay.dshackle.upstream.calls

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

class DirectCallMethodsTest : ShouldSpec({

    context("equals") {
        should("equal empty") {
            val methodsOne = DirectCallMethods()
            val methodsTwo = DirectCallMethods()

            val equal = methodsOne == methodsTwo && methodsTwo == methodsOne
            equal shouldBe true
        }

        should("equal with methods") {
            val methodsOne = DirectCallMethods(setOf("one", "two", "three"))
            val methodsTwo = DirectCallMethods(setOf("one", "three", "two"))

            val equal = methodsOne == methodsTwo && methodsTwo == methodsOne
            equal shouldBe true
        }

        should("not equal different") {
            val methodsOne = DirectCallMethods(setOf("one", "two", "three"))
            val methodsTwo = DirectCallMethods(setOf("one", "three", "four"))

            val equalOne = methodsOne == methodsTwo
            equalOne shouldNotBe true

            val equalTwo = methodsTwo == methodsOne
            equalTwo shouldNotBe true
        }
    }
})
