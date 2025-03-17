package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.ThrottledLogger
import io.emeraldpay.dshackle.upstream.Head
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.time.Duration
import java.time.Instant

class HeadLivenessValidatorImpl(
    private val head: Head,
    private val expectedBlockTime: Duration,
    private val scheduler: Scheduler,
    private val upstreamId: String,
) : HeadLivenessValidator {
    companion object {
        const val CHECKED_BLOCKS_UNTIL_LIVE = 3
        const val COOLDOWN_MINUTES = 5L
        private val log = LoggerFactory.getLogger(HeadLivenessValidatorImpl::class.java)
    }

    @Volatile
    private var lastNonConsecutiveTime: Instant? = null

    override fun getFlux(): Flux<HeadLivenessState> {
        val headLiveness = head.headLiveness()
        // first we have moving window of 2 blocks and check that they are consecutive ones
        val headFlux = head.getFlux().map { it.height }.buffer(2, 1).map {
            it.last() - it.first() == 1L
        }.scan(Pair(0, true)) { acc, value ->
            // then we accumulate consecutive true events, false resets counter
            if (value) {
                Pair(acc.first + 1, true)
            } else {
                if (log.isDebugEnabled) {
                    log.debug("non consecutive blocks in head for $upstreamId")
                } else {
                    ThrottledLogger.log(log, "non consecutive blocks in head for $upstreamId")
                }
                // Mark the time when we detected non-consecutive blocks
                lastNonConsecutiveTime = Instant.now()
                Pair(0, false)
            }
        }.flatMap { (count, value) ->
            // we emit when we have false or checked CHECKED_BLOCKS_UNTIL_LIVE blocks
            // CHECKED_BLOCKS_UNTIL_LIVE blocks == (CHECKED_BLOCKS_UNTIL_LIVE - 1) consecutive true
            when {
                !value -> {
                    Flux.just(HeadLivenessState.NON_CONSECUTIVE)
                }
                count >= (CHECKED_BLOCKS_UNTIL_LIVE - 1) -> {
                    // Check if we're still in the cooldown period
                    val lastNonConsec = lastNonConsecutiveTime
                    if (lastNonConsec != null && Duration.between(lastNonConsec, Instant.now()).toMinutes() < COOLDOWN_MINUTES) {
                        if (log.isDebugEnabled) {
                            log.debug("Still in cooldown period for $upstreamId after non-consecutive blocks")
                        }
                        Flux.just(HeadLivenessState.NON_CONSECUTIVE)
                    } else {
                        Flux.just(HeadLivenessState.OK)
                    }
                }
                else -> Flux.empty()
            }
        }.timeout(
            expectedBlockTime.multipliedBy(CHECKED_BLOCKS_UNTIL_LIVE.toLong() * 2),
            Flux.just(HeadLivenessState.NON_CONSECUTIVE).doOnNext {
                if (log.isDebugEnabled) {
                    log.debug("head liveness check broken with timeout in $upstreamId")
                } else {
                    ThrottledLogger.log(log, "head liveness check broken with timeout in $upstreamId")
                }
            },
        ).repeat().subscribeOn(scheduler)

        return Flux.merge(headFlux, headLiveness)
    }
}
