package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.upstream.EmptyHead
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class DataReadersTest :
    ShouldSpec({

        val blockJson =
            DataReadersTest::class.java.classLoader
                .getResourceAsStream("bitcoin/block-626472.json")
                .readAllBytes()
        val source = mockk<DshackleRpcReader>()
        every {
            source.read(
                DshackleRequest("getblock", listOf("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90", 1)),
            )
        } returns
            Mono.just(DshackleResponse(1, blockJson))
        every { source.read(DshackleRequest("getblockhash", listOf(626472L))) } returns
            Mono.just(DshackleResponse(1, "\"0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90\"".toByteArray()))

        should("fetch a block") {
            val dataReaders = DataReaders(source, AtomicReference(EmptyHead()))

            val act =
                dataReaders
                    .getBlock(BlockId.from("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"), 1)
                    .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act.hash shouldBe BlockId.from("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90")
        }

        should("fetch a parsed block") {
            val dataReaders = DataReaders(source, AtomicReference(EmptyHead()))

            val act =
                dataReaders
                    .getBlockJson(BlockId.from("0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"))
                    .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act["hash"] shouldBe "0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"
        }

        should("fetch a parsed block by height") {
            val dataReaders = DataReaders(source, AtomicReference(EmptyHead()))

            val act =
                dataReaders
                    .getBlockJson(626472)
                    .block(Duration.ofSeconds(1))

            act shouldNotBe null
            act["hash"] shouldBe "0000000000000000000889c2e52ca5e1cecac60bce9a3754201a7a9a67791e90"
        }
    })
