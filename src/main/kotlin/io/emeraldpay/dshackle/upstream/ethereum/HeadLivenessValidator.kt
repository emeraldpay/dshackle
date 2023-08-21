package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.Head
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.time.Duration

class HeadLivenessValidator(
    private val head: Head,
    private val expectedBlockTime: Duration,
    private val scheduler: Scheduler,
    private val upstreamId: String
) {
    companion object {
        const val CHECKED_BLOCKS_UNTIL_LIVE = 3
        private val log = LoggerFactory.getLogger(HeadLivenessValidator::class.java)
    }

    fun getFlux(): Flux<Boolean> {
        // first we have moving window of 2 blocks and check that they are consecutive ones
        return head.getFlux().map { it.height }.buffer(2, 1).map {
            it.last() - it.first() == 1L
        }.scan(Pair(0, true)) { acc, value ->
            // then we accumulate consecutive true events, false resets counter
            if (value) {
                Pair(acc.first + 1, true)
            } else {
                log.debug("non consecutive blocks in head for $upstreamId")
                Pair(0, false)
            }
        }.flatMap { (count, value) ->
            // we emit when we have false or checked CHECKED_BLOCKS_UNTIL_LIVE blocks
            // CHECKED_BLOCKS_UNTIL_LIVE blocks == (CHECKED_BLOCKS_UNTIL_LIVE - 1) consecutive true
            when {
                count >= (CHECKED_BLOCKS_UNTIL_LIVE - 1) -> Flux.just(true)
                !value -> Flux.just(false)
                else -> Flux.empty()
            }
        }.timeout(
            expectedBlockTime.multipliedBy(CHECKED_BLOCKS_UNTIL_LIVE.toLong() * 2),
            Flux.just(false).doOnNext {
                log.debug("head liveness check broken with timeout in $upstreamId")
            }
        ).repeat().subscribeOn(scheduler)
    }
}
