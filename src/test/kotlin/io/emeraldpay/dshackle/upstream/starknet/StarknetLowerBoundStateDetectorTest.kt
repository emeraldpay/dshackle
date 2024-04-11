package io.emeraldpay.dshackle.upstream.starknet

import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.time.Duration

class StarknetLowerBoundStateDetectorTest {

    @Test
    fun `starknet lower block is 1`() {
        val detector = StarknetLowerBoundStateDetector()

        StepVerifier.withVirtualTime { detector.detectLowerBound() }
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(15))
            .expectNext(LowerBoundData(1, LowerBoundType.STATE))
            .thenCancel()
            .verify(Duration.ofSeconds(3))
    }
}
