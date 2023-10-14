package io.emeraldpay.dshackle.upstream

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.test.ForkWatchMock
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import java.time.Duration

class DefaultUpstreamTest : ShouldSpec({

    should("Produce current status availability") {
        val upstream = DefaultUpstreamTestImpl("test", ForkWatch.Never())
        upstream.setStatus(UpstreamAvailability.LAGGING)
        val act = upstream.availabilityByStatus.take(1)
            .collectList().block(Duration.ofSeconds(1))
        act shouldBe listOf(UpstreamAvailability.LAGGING)
    }

    should("Produce updated status availability") {
        val upstream = DefaultUpstreamTestImpl("test", ForkWatch.Never())
        upstream.setStatus(UpstreamAvailability.LAGGING)
        val act = upstream.availabilityByStatus.take(2)

        StepVerifier.create(act)
            .expectNext(UpstreamAvailability.LAGGING)
            .then {
                upstream.setStatus(UpstreamAvailability.OK)
            }
            // syncing because we didn't set the height lag
            .expectNext(UpstreamAvailability.SYNCING)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Availability for always valid upstream doesn't react on old height") {
        val upstream = DefaultUpstreamTestImpl(
            "test",
            ForkWatch.Never(),
            UpstreamsConfig.PartialOptions.getDefaults().apply {
                disableValidation = true
            }.build()
        )
        // when have a zero height
        upstream.setStatus(UpstreamAvailability.OK)
        val act = upstream.getStatus()
        act shouldBe UpstreamAvailability.OK

        // when we set large lag
        upstream.setLag(100)
        upstream.setStatus(UpstreamAvailability.OK)
        val act2 = upstream.getStatus()
        act2 shouldBe UpstreamAvailability.OK
    }

    should("Turn off always valid upstream without making it as lagging") {
        val upstream = DefaultUpstreamTestImpl(
            "test",
            ForkWatch.Never(),
            UpstreamsConfig.PartialOptions.getDefaults().apply {
                disableValidation = true
            }.build()
        )
        upstream.setStatus(UpstreamAvailability.UNAVAILABLE)
        val act = upstream.availabilityByStatus.take(3)

        StepVerifier.create(act)
            .expectNext(UpstreamAvailability.UNAVAILABLE)
            .then {
                upstream.setStatus(UpstreamAvailability.OK)
            }
            .expectNext(UpstreamAvailability.OK)
            .then {
                upstream.setStatus(UpstreamAvailability.OK)
            }
            .expectNoEvent(Duration.ofMillis(50))
            .then {
                upstream.setStatus(UpstreamAvailability.UNAVAILABLE)
            }
            .expectNext(UpstreamAvailability.UNAVAILABLE)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Availability by forks produce current status") {
        val upstream = DefaultUpstreamTestImpl("test", ForkWatch.Never())
        val act = upstream.availabilityByForks.take(1)
            .collectList().block(Duration.ofSeconds(1))
        act shouldBe listOf(UpstreamAvailability.OK)
    }

    should("Availability by forks produce updated status") {
        val results = Sinks.many().unicast().onBackpressureBuffer<Boolean>()
        val upstream = DefaultUpstreamTestImpl("test", ForkWatchMock(results.asFlux()))

        val act = upstream.availabilityByForks.take(2)

        StepVerifier.create(act)
            .expectNext(UpstreamAvailability.OK)
            .then {
                results.tryEmitNext(true)
            }
            .expectNext(UpstreamAvailability.IMMATURE)
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    should("Http status 429 should disable upstream") {
        val upstream = DefaultUpstreamTestImpl("test", ForkWatch.Never())
        upstream.setStatus(UpstreamAvailability.OK)
        upstream.watchHttpCodes.accept(429)
        upstream.getStatus() shouldBe UpstreamAvailability.UNAVAILABLE
    }
})

class DefaultUpstreamTestImpl(
    id: String,
    forkWatch: ForkWatch,
    options: UpstreamsConfig.Options = UpstreamsConfig.PartialOptions.getDefaults().build()
) : DefaultUpstream(
    id, Chain.ETHEREUM, forkWatch, options, UpstreamsConfig.UpstreamRole.PRIMARY, DefaultEthereumMethods(Chain.ETHEREUM)
) {
    override fun getHead(): Head {
        throw NotImplementedError()
    }

    override fun getIngressReader(): StandardRpcReader {
        throw NotImplementedError()
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        throw NotImplementedError()
    }

    override fun getCapabilities(): Set<Capability> {
        throw NotImplementedError()
    }

    override fun isGrpc(): Boolean {
        return false
    }

    override fun <T : Upstream> cast(selfType: Class<T>): T {
        throw NotImplementedError()
    }
}
