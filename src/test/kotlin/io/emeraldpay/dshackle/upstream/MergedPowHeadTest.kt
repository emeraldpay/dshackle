package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.kotest.core.spec.style.ShouldSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.springframework.context.Lifecycle
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.math.BigInteger
import java.time.Duration
import java.time.Instant

class MergedPowHeadTest : ShouldSpec({

    val blocks = (10L..20L).map { i ->
        BlockContainer.from(
            BlockJson<TransactionRefJson>().apply {
                number = 10000L + i
                hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec81111" + i)
                totalDifficulty = BigInteger.valueOf(11 * i)
                timestamp = Instant.now()
            }
        )
    }

    should("Start delegates") {
        val head1 = mockk<StartableHead>(relaxed = true)
        val head2 = mockk<Head>()
        val head = MergedPowHead(listOf(head1, head2))

        every { head1.isRunning } returns false
        every { head1.getFlux() } returns Flux.never<BlockContainer>()
        every { head2.getFlux() } returns Flux.never<BlockContainer>()

        head.start()

        verify { head1.start() }
    }

    should("Follow") {
        val head = MergedPowHead(emptyList())

        val act = head.merge(listOf(Flux.just(blocks[0])))
            .take(Duration.ofMillis(500))

        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Follow normal order") {
        val head = MergedPowHead(emptyList())
        val act = head.merge(listOf(Flux.just(blocks[0], blocks[1], blocks[3])))
            .take(Duration.ofMillis(500))

        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectNext(blocks[1])
            .expectNext(blocks[3])
            .expectComplete()
            .verify(Duration.ofSeconds(100))
    }

    should("Ignore old") {
        val head = MergedPowHead(emptyList())
        val act = head.merge(listOf(Flux.just(blocks[0], blocks[3], blocks[1])))
            .take(Duration.ofMillis(500))
        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectNext(blocks[3])
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Ignore repeating") {
        val head = MergedPowHead(emptyList())
        val act = head.merge(listOf(Flux.just(blocks[0], blocks[3], blocks[3], blocks[3], blocks[2], blocks[3])))
            .take(Duration.ofMillis(500))
        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectNext(blocks[3])
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Ignore less difficult") {
        val block3less = BlockContainer.from(
            BlockJson<TransactionRefJson>().apply {
                number = blocks[3].height
                hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8000000")
                totalDifficulty = blocks[3].difficulty - BigInteger.ONE
                timestamp = Instant.now()
            }
        )

        val head = MergedPowHead(emptyList())
        val act = head.merge(listOf(Flux.just(blocks[0], blocks[3], block3less)))
            .take(Duration.ofMillis(500))
        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectNext(blocks[3])
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Replace with more difficult") {
        val head = MergedPowHead(emptyList())
        val block3less = BlockContainer.from(
            BlockJson<TransactionRefJson>().apply {
                number = blocks[3].height
                hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec8000000")
                totalDifficulty = blocks[3].difficulty + BigInteger.ONE
                timestamp = Instant.now()
            }
        )

        val act = head.merge(listOf(Flux.just(blocks[0], blocks[3], block3less)))
            .take(Duration.ofMillis(500))
        StepVerifier.create(act)
            .expectNext(blocks[0])
            .expectNext(blocks[3])
            .expectNext(block3less)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }
})

interface StartableHead : Head, Lifecycle
