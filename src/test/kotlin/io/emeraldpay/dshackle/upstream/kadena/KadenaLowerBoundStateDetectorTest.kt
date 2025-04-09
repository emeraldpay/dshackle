package io.emeraldpay.dshackle.upstream.kadena

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.time.Duration

class KadenaLowerBoundStateDetectorTest {

    @Test
    fun `kadena lower block is 1`() {
        val detector = KadenaLowerBoundStateDetector(Chain.KADENA__MAINNET)

        StepVerifier.withVirtualTime { detector.detectLowerBound() }
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(15))
            .expectNext(LowerBoundData(1, LowerBoundType.STATE))
            .thenCancel()
            .verify(Duration.ofSeconds(3))
    }
}
