package io.emeraldpay.dshackle.upstream.state

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import reactor.test.StepVerifier
import java.time.Duration

class MultistreamStateTest {

    @Test
    fun `update state and send events`() {
        val up1 = upstream(
            UpstreamAvailability.OK,
            true,
            callMethods(setOf("eth_call", "super_call")),
            setOf(Capability.RPC),
            QuorumForLabels(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "value")))),
            listOf(LowerBoundData(55, LowerBoundType.STATE), LowerBoundData(99, LowerBoundType.BLOCK)),
            listOf(FinalizationData(32, FinalizationType.SAFE_BLOCK), FinalizationData(80, FinalizationType.FINALIZED_BLOCK)),
        )
        val up2 = upstream(
            UpstreamAvailability.UNAVAILABLE,
            false,
            callMethods(setOf("one_more_call")),
            setOf(Capability.BALANCE),
            QuorumForLabels(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("new" to "old")))),
            listOf(LowerBoundData(22, LowerBoundType.STATE), LowerBoundData(5, LowerBoundType.BLOCK)),
            listOf(FinalizationData(3200, FinalizationType.SAFE_BLOCK), FinalizationData(1180, FinalizationType.FINALIZED_BLOCK)),
        )
        val up3 = upstream(
            UpstreamAvailability.LAGGING,
            true,
            callMethods(setOf("super_duper_call")),
            setOf(Capability.WS_HEAD),
            QuorumForLabels(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("megaTest" to "valTest")))),
            listOf(LowerBoundData(40, LowerBoundType.STATE), LowerBoundData(400, LowerBoundType.BLOCK)),
            listOf(FinalizationData(70, FinalizationType.SAFE_BLOCK), FinalizationData(15, FinalizationType.FINALIZED_BLOCK)),
        )
        val up4 = upstream(
            UpstreamAvailability.OK,
            true,
            callMethods(setOf("super_duper_call", "yet_another_call")),
            setOf(Capability.WS_HEAD),
            QuorumForLabels(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("megaTest" to "valTest")))),
            listOf(LowerBoundData(1, LowerBoundType.STATE), LowerBoundData(1, LowerBoundType.BLOCK)),
            listOf(FinalizationData(990, FinalizationType.SAFE_BLOCK), FinalizationData(880, FinalizationType.FINALIZED_BLOCK)),
        )

        val state = MultistreamState {}

        StepVerifier.create(state.stateEvents())
            .then { state.updateState(listOf(up1, up2, up3), listOf("heads", "notHeads")) }
            .assertNext {
                assertThat(it).hasSize(7)
                assertThat(it.toList())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lowerBounds.timestamp")
                    .hasSameElementsAs(
                        listOf(
                            MultistreamStateEvent.StatusEvent(UpstreamAvailability.OK),
                            MultistreamStateEvent.MethodsEvent(setOf("eth_call", "super_call", "super_duper_call").toHashSet()),
                            MultistreamStateEvent.SubsEvent(listOf("heads", "notHeads")),
                            MultistreamStateEvent.CapabilitiesEvent(setOf(Capability.WS_HEAD, Capability.RPC).toHashSet()),
                            MultistreamStateEvent.LowerBoundsEvent(
                                setOf(
                                    LowerBoundData(40, LowerBoundType.STATE),
                                    LowerBoundData(99, LowerBoundType.BLOCK),
                                ).toHashSet(),
                            ),
                            MultistreamStateEvent.FinalizationEvent(
                                setOf(
                                    FinalizationData(70, FinalizationType.SAFE_BLOCK),
                                    FinalizationData(80, FinalizationType.FINALIZED_BLOCK),
                                ).toHashSet(),
                            ),
                            MultistreamStateEvent.NodeDetailsEvent(
                                listOf(
                                    QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "value"))),
                                    QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("megaTest" to "valTest"))),
                                ),
                            ),
                        ),
                    )
            }
            .then { state.updateState(listOf(up1, up2, up3), listOf("heads", "notHeads")) }
            .assertNext {
                assertThat(it).hasSize(0)
            }
            .then { state.updateState(listOf(up1, up2, up3, up4), listOf("heads", "notHeads")) }
            .assertNext {
                assertThat(it).hasSize(4)
                assertThat(it.toList())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lowerBounds.timestamp")
                    .hasSameElementsAs(
                        listOf(
                            MultistreamStateEvent.MethodsEvent(setOf("eth_call", "yet_another_call", "super_call", "super_duper_call").toHashSet()),
                            MultistreamStateEvent.LowerBoundsEvent(
                                setOf(
                                    LowerBoundData(1, LowerBoundType.STATE),
                                    LowerBoundData(1, LowerBoundType.BLOCK),
                                ).toHashSet(),
                            ),
                            MultistreamStateEvent.FinalizationEvent(
                                setOf(
                                    FinalizationData(990, FinalizationType.SAFE_BLOCK),
                                    FinalizationData(880, FinalizationType.FINALIZED_BLOCK),
                                ).toHashSet(),
                            ),
                            MultistreamStateEvent.NodeDetailsEvent(
                                listOf(
                                    QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "value"))),
                                    QuorumForLabels.QuorumItem(2, UpstreamsConfig.Labels.fromMap(mapOf("megaTest" to "valTest"))),
                                ),
                            ),
                        ),
                    )
            }
            .thenCancel()
            .verify(Duration.ofSeconds(1))
    }

    private fun upstream(
        status: UpstreamAvailability,
        isAvailable: Boolean,
        callMethods: CallMethods,
        capabilities: Set<Capability>,
        quorumForLabels: QuorumForLabels,
        lowerBounds: Collection<LowerBoundData>,
        finalizationData: Collection<FinalizationData>,
    ): DefaultUpstream {
        val upstream = mock<DefaultUpstream> {
            on { isAvailable() } doReturn isAvailable
            on { getMethods() } doReturn callMethods
            on { getCapabilities() } doReturn capabilities
            on { getQuorumByLabel() } doReturn quorumForLabels
            on { getLowerBounds() } doReturn lowerBounds
            on { getFinalizations() } doReturn finalizationData
            on { getStatus() } doReturn status
        }

        return upstream
    }

    private fun callMethods(methods: Set<String>): CallMethods {
        val callMethods = mock<CallMethods> {
            on { getSupportedMethods() } doReturn methods
        }
        return callMethods
    }
}
