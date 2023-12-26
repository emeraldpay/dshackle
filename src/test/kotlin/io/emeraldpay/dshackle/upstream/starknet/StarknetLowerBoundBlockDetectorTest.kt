package io.emeraldpay.dshackle.upstream.starknet

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import reactor.test.StepVerifier
import java.time.Duration

class StarknetLowerBoundBlockDetectorTest {

    @Test
    fun `starknet lower block is 1`() {
        val detector = StarknetLowerBoundBlockDetector(Chain.UNSPECIFIED, mock<Upstream>())

        StepVerifier.withVirtualTime { detector.lowerBlock() }
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(15))
            .expectNext(LowerBoundBlockDetector.LowerBlockData(1))
            .thenCancel()
            .verify(Duration.ofSeconds(3))
    }
}
