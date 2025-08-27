package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import io.kotest.core.spec.style.ShouldSpec
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import java.math.BigInteger
import java.time.Duration
import java.time.Instant

class MergedPosHeadTest :
    ShouldSpec({

        val blocks =
            (10L..20L).map { i ->
                BlockContainer.from(
                    BlockJson<TransactionRefJson>().apply {
                        number = 10000L + i
                        hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec81111" + i)
                        totalDifficulty = BigInteger.ONE
                        timestamp = Instant.now()
                    },
                )
            }

        val blocksOther =
            (10L..20L).map { i ->
                BlockContainer.from(
                    BlockJson<TransactionRefJson>().apply {
                        number = 10000L + i
                        hash = BlockHash.from("0x3ec2ebf5d0ec474d0ac6bc50d2770d8409ad76e119968e7919f85d5ec82222" + i)
                        totalDifficulty = BigInteger.ONE
                        timestamp = Instant.now()
                    },
                )
            }

        should("Follow normal order") {
            val head = MergedPosHead(emptyList())
            val act =
                head
                    .merge(
                        listOf(
                            Pair(1, Flux.just(blocks[0], blocks[1], blocks[3]).delayElements(Duration.ofMillis(10))),
                            Pair(2, Flux.just(blocks[0], blocks[1], blocks[3]).delayElements(Duration.ofMillis(10))),
                        ),
                    ).take(Duration.ofMillis(500))

            StepVerifier
                .create(act)
                .expectNext(blocks[0])
                .expectNext(blocks[1])
                .expectNext(blocks[3])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
        }

        should("Keep higher priority") {
            val head = MergedPosHead(emptyList())

            val blocks1 = Sinks.many().unicast().onBackpressureBuffer<BlockContainer>()
            val blocks2 = Sinks.many().unicast().onBackpressureBuffer<BlockContainer>()
            val stream =
                head
                    .merge(
                        listOf(
                            Pair(2, blocks1.asFlux()),
                            Pair(1, blocks2.asFlux()),
                        ),
                    ).take(Duration.ofMillis(500))

            StepVerifier
                .create(stream)
                .then {
                    blocks1.tryEmitNext(blocks[0])
                    blocks2.tryEmitNext(blocksOther[0])
                }.expectNext(blocks[0])
                .then {
                    blocks2.tryEmitNext(blocks[1])
                    blocks1.tryEmitNext(blocks[1])
                }.expectNext(blocks[1])
                .then {
                    blocks1.tryEmitNext(blocks[2])
                    blocks2.tryEmitNext(blocksOther[2])
                }.expectNext(blocks[2])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
        }

        should("Restart if filled with low priority") {
            val head = MergedPosHead(emptyList())

            val blocks1 = Sinks.many().unicast().onBackpressureBuffer<BlockContainer>()
            val blocks2 = Sinks.many().unicast().onBackpressureBuffer<BlockContainer>()
            val stream =
                head
                    .merge(
                        listOf(
                            Pair(2, blocks1.asFlux()),
                            Pair(1, blocks2.asFlux()),
                        ),
                    ).take(Duration.ofMillis(500))

            StepVerifier
                .create(stream)
                .then {
                    blocks2.tryEmitNext(blocksOther[3])
                }.expectNext(blocksOther[3])
                .then {
                    blocks2.tryEmitNext(blocksOther[4])
                }.expectNext(blocksOther[4])
                .then {
                    blocks1.tryEmitNext(blocks[2])
                }
                // a block from the high priority source
                .expectNext(blocks[2])
                .expectComplete()
                .verify(Duration.ofSeconds(1))
        }
    })
