package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Mono
import java.time.Duration

class BitcoinCacheReaderTest : ShouldSpec({

    should("Tx request should accept params without verbosity") {
        val reader = mockk<BitcoinCacheReader>()

        every { reader.processGetTxRequest(any()) } answers { callOriginal() }
        every { reader.readRawTx(TxId.from("37ba87c0c8735341f4a134411cf3c54bc16c734882d3c7eb935d1423914fc88c"), false) } returns
            Mono.just("ok".toByteArray())

        val act = reader.processGetTxRequest(DshackleRequest("getrawtransaction", listOf("37ba87c0c8735341f4a134411cf3c54bc16c734882d3c7eb935d1423914fc88c")))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        String(act) shouldBe "ok"
    }

    should("Tx request should accept params with verbosity as false") {
        val reader = mockk<BitcoinCacheReader>()

        every { reader.processGetTxRequest(any()) } answers { callOriginal() }
        every { reader.readRawTx(TxId.from("37ba87c0c8735341f4a134411cf3c54bc16c734882d3c7eb935d1423914fc88c"), false) } returns
            Mono.just("ok-false".toByteArray())

        val act = reader.processGetTxRequest(DshackleRequest("getrawtransaction", listOf("37ba87c0c8735341f4a134411cf3c54bc16c734882d3c7eb935d1423914fc88c", false)))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        String(act) shouldBe "ok-false"
    }

    should("Tx request should accept params with verbosity as true") {
        val reader = mockk<BitcoinCacheReader>()

        every { reader.processGetTxRequest(any()) } answers { callOriginal() }
        every { reader.readRawTx(TxId.from("37ba87c0c8735341f4a134411cf3c54bc16c734882d3c7eb935d1423914fc88c"), true) } returns
            Mono.just("ok-true".toByteArray())

        val act = reader.processGetTxRequest(DshackleRequest("getrawtransaction", listOf("37ba87c0c8735341f4a134411cf3c54bc16c734882d3c7eb935d1423914fc88c", true)))
            .block(Duration.ofSeconds(1))

        act shouldNotBe null
        String(act) shouldBe "ok-true"
    }
})
