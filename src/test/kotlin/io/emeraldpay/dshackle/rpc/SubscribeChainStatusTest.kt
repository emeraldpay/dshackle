package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HeadLagObserver
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.state.MultistreamStateEvent
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import java.math.BigInteger
import java.time.Duration
import java.time.Instant

class SubscribeChainStatusTest {
    private val chainEventMapper = ChainEventMapper()

    @Test
    fun `terminate stream if an error is thrown`() {
        val head = mock<Head> {
            on { getCurrent() } doReturn null
            on { getFlux() } doReturn Flux.error(IllegalStateException())
        }
        val ms = mock<Multistream> {
            on { chain } doReturn Chain.ETHEREUM__MAINNET
            on { getHead() } doReturn head
            on { stateEvents() } doReturn Flux.empty()
        }
        val msHolder = mock<MultistreamHolder> {
            on { all() } doReturn listOf(ms)
        }
        val subscribeChainStatus = SubscribeChainStatus(msHolder, chainEventMapper)

        StepVerifier.create(subscribeChainStatus.chainStatuses())
            .expectSubscription()
            .expectError()
            .verify(Duration.ofSeconds(1))
    }

    @Test
    fun `first full event if there is already an ms head`() {
        val head = mock<Head> {
            on { getCurrent() } doReturn head(550)
            on { getFlux() } doReturn Flux.empty()
        }
        val ms = spy<TestMultistream> {
            on { getHead() } doReturn head
            on { stateEvents() } doReturn Flux.empty()
        }
        val msHolder = mock<MultistreamHolder> {
            on { all() } doReturn listOf(ms)
        }
        val subscribeChainStatus = SubscribeChainStatus(msHolder, chainEventMapper)

        StepVerifier.create(subscribeChainStatus.chainStatuses())
            .expectSubscription()
            .expectNext(response(true))
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    @Test
    fun `first full event with awaiting a head from a head stream`() {
        val head = mock<Head> {
            on { getCurrent() } doReturn null
            on { getFlux() } doReturn Flux.just(head(550))
        }
        val ms = spy<TestMultistream> {
            on { getHead() } doReturn head
            on { stateEvents() } doReturn Flux.empty()
        }
        val msHolder = mock<MultistreamHolder> {
            on { all() } doReturn listOf(ms)
        }
        val subscribeChainStatus = SubscribeChainStatus(msHolder, chainEventMapper)

        StepVerifier.create(subscribeChainStatus.chainStatuses())
            .expectSubscription()
            .expectNext(response(true))
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    @Test
    fun `first full event with awaiting a head from a head stream and then state events`() {
        val head = mock<Head> {
            on { getCurrent() } doReturn null
            on { getFlux() } doReturn Flux.just(head(550))
        }
        val ms = spy<TestMultistream> {
            on { getHead() } doReturn head
            on { stateEvents() } doReturn Flux.just(
                listOf(
                    MultistreamStateEvent.StatusEvent(UpstreamAvailability.OK),
                    MultistreamStateEvent.MethodsEvent(setOf("superMethod")),
                    MultistreamStateEvent.SubsEvent(listOf("heads")),
                    MultistreamStateEvent.CapabilitiesEvent(setOf(Capability.BALANCE)),
                    MultistreamStateEvent.LowerBoundsEvent(listOf(LowerBoundData(800, 1000, LowerBoundType.STATE))),
                    MultistreamStateEvent.FinalizationEvent(listOf(FinalizationData(30, FinalizationType.SAFE_BLOCK))),
                    MultistreamStateEvent.NodeDetailsEvent(listOf(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "val"))))),
                ),
            )
        }
        val msHolder = mock<MultistreamHolder> {
            on { all() } doReturn listOf(ms)
        }
        val subscribeChainStatus = SubscribeChainStatus(msHolder, chainEventMapper)

        StepVerifier.create(subscribeChainStatus.chainStatuses())
            .expectSubscription()
            .expectNext(response(true))
            .expectNext(response(false))
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    @Test
    fun `first full event with awaiting a head from a head stream and then head events`() {
        val head = mock<Head> {
            on { getCurrent() } doReturn null
            on { getFlux() } doReturn Flux.just(head(550), head(600))
        }
        val ms = spy<TestMultistream> {
            on { getHead() } doReturn head
            on { stateEvents() } doReturn Flux.empty()
        }
        val msHolder = mock<MultistreamHolder> {
            on { all() } doReturn listOf(ms)
        }
        val subscribeChainStatus = SubscribeChainStatus(msHolder, chainEventMapper)

        StepVerifier.create(subscribeChainStatus.chainStatuses())
            .expectSubscription()
            .expectNext(response(true))
            .expectNext(
                BlockchainOuterClass.SubscribeChainStatusResponse.newBuilder()
                    .setChainDescription(
                        BlockchainOuterClass.ChainDescription.newBuilder()
                            .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                            .addChainEvent(chainEventMapper.mapHead(head(600)))
                            .build(),
                    )
                    .build(),
            )
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    private fun head(height: Long): BlockContainer {
        return BlockContainer(
            height,
            BlockId.from("0xa6af163aab691919c595e2a466f0a7b01f1dff8cfd9631dee811df57064c2d32"),
            BigInteger.ONE,
            Instant.ofEpochSecond(1719485864),
            false,
            null,
            null,
            BlockId.from("0xa6af163aab691919c595e2a466f0a6b01f1dff8cfd9631dee811df57064c2d32"),
            emptyList(),
        )
    }

    private fun response(headEvent: Boolean): BlockchainOuterClass.SubscribeChainStatusResponse {
        return BlockchainOuterClass.SubscribeChainStatusResponse.newBuilder()
            .apply {
                if (headEvent) {
                    setBuildInfo(
                        BlockchainOuterClass.BuildInfo.newBuilder()
                            .setVersion("DEV")
                            .build(),
                    )
                }
            }
            .setChainDescription(
                BlockchainOuterClass.ChainDescription.newBuilder()
                    .setChain(Common.ChainRef.CHAIN_ETHEREUM__MAINNET)
                    .addChainEvent(chainEventMapper.chainStatus(UpstreamAvailability.OK))
                    .apply {
                        if (headEvent) {
                            addChainEvent(chainEventMapper.mapHead(head(550)))
                        }
                    }
                    .addChainEvent(chainEventMapper.supportedMethods(setOf("superMethod")))
                    .addChainEvent(chainEventMapper.supportedSubs(listOf("heads")))
                    .addChainEvent(chainEventMapper.mapCapabilities(setOf(Capability.BALANCE)))
                    .addChainEvent(
                        chainEventMapper.mapLowerBounds(
                            listOf(LowerBoundData(800, 1000, LowerBoundType.STATE)),
                        ),
                    )
                    .addChainEvent(
                        chainEventMapper.mapFinalizationData(
                            listOf(FinalizationData(30, FinalizationType.SAFE_BLOCK)),
                        ),
                    )
                    .addChainEvent(
                        chainEventMapper.mapNodeDetails(
                            listOf(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "val")))),
                        ),
                    )
                    .build(),
            )
            .apply {
                if (headEvent) {
                    setFullResponse(true)
                }
            }
            .build()
    }

    private open class TestMultistream : Multistream(Chain.ETHEREUM__MAINNET, mock<Caches>(), null, Schedulers.single()) {
        companion object {
            private const val UNIMPLEMENTED = "UNIMPLEMENTED"
        }

        override fun getMethods(): CallMethods {
            val callMethods = mock<CallMethods> {
                on { getSupportedMethods() } doReturn setOf("superMethod")
            }
            return callMethods
        }

        override fun getUpstreams(): MutableList<out Upstream> {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun addUpstreamInternal(u: Upstream) {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun getLocalReader(): Mono<ChainReader> {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun addHead(upstream: Upstream) {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun removeHead(upstreamId: String) {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun makeLagObserver(): HeadLagObserver {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun getCachingReader(): CachingReader? {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun getHead(mather: Selector.Matcher): Head {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun getHead(): Head {
            return mock<Head>()
        }

        override fun getEgressSubscription(): EgressSubscription {
            val sub = mock<EgressSubscription> {
                on { getAvailableTopics() } doReturn listOf("heads")
            }
            return sub
        }

        override fun getLabels(): Collection<UpstreamsConfig.Labels> {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun <T : Upstream> cast(selfType: Class<T>): T {
            throw IllegalStateException(UNIMPLEMENTED)
        }

        override fun getStatus(): UpstreamAvailability {
            return UpstreamAvailability.OK
        }

        override fun getCapabilities(): Set<Capability> {
            return setOf(Capability.BALANCE)
        }

        override fun getLowerBounds(): Collection<LowerBoundData> {
            return listOf(LowerBoundData(800, 1000, LowerBoundType.STATE))
        }

        override fun getFinalizations(): Collection<FinalizationData> {
            return listOf(FinalizationData(30, FinalizationType.SAFE_BLOCK))
        }

        override fun getQuorumLabels(): List<QuorumForLabels.QuorumItem> {
            return listOf(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "val"))))
        }
    }
}
