package io.emeraldpay.dshackle.data

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe

class BlockContainerTest :
    ShouldSpec({

        should("Parse a large block json") {
            val raw =
                javaClass.getResource("/ethereum/block-23122116.json")?.readBytes()
                    ?: error("Failed to load test data")

            val block = BlockContainer.fromEthereumJson(raw)

            block.height shouldBe 23122116L
        }
    })
