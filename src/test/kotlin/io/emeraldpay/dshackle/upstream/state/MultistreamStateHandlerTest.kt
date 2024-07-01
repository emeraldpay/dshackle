package io.emeraldpay.dshackle.upstream.state

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class MultistreamStateHandlerTest {
    private val stateHandler = MultistreamStateHandler

    @ParameterizedTest
    @MethodSource("states")
    fun `compare states`(
        newState: MultistreamState.CurrentMultistreamState,
        expectedEvents: Collection<MultistreamStateEvent>,
    ) {
        val oldState = state()
        val events = stateHandler.compareStates(oldState, newState)

        assertThat(events).isEqualTo(expectedEvents)
    }

    companion object {
        @JvmStatic
        fun states(): List<Arguments> =
            listOf(
                Arguments.of(
                    state(),
                    listOf<MultistreamStateEvent>(),
                ),
                Arguments.of(
                    state(status = UpstreamAvailability.UNAVAILABLE),
                    listOf(MultistreamStateEvent.StatusEvent(UpstreamAvailability.UNAVAILABLE)),
                ),
                Arguments.of(
                    state(methods = setOf("otherCall")),
                    listOf(MultistreamStateEvent.MethodsEvent(setOf("otherCall"))),
                ),
                Arguments.of(
                    state(subs = setOf("newSub")),
                    listOf(MultistreamStateEvent.SubsEvent(setOf("newSub"))),
                ),
                Arguments.of(
                    state(caps = setOf(Capability.BALANCE)),
                    listOf(MultistreamStateEvent.CapabilitiesEvent(setOf(Capability.BALANCE))),
                ),
                Arguments.of(
                    state(lowerBounds = setOf(LowerBoundData(90, 90, LowerBoundType.STATE))),
                    listOf(MultistreamStateEvent.LowerBoundsEvent(setOf(LowerBoundData(90, 90, LowerBoundType.STATE)))),
                ),
                Arguments.of(
                    state(finalizationData = setOf(FinalizationData(80, FinalizationType.SAFE_BLOCK))),
                    listOf(MultistreamStateEvent.FinalizationEvent(setOf(FinalizationData(80, FinalizationType.SAFE_BLOCK)))),
                ),
                Arguments.of(
                    state(quorumForLabels = setOf(QuorumForLabels.QuorumItem(2, UpstreamsConfig.Labels.fromMap(mapOf("test1" to "val1"))))),
                    listOf(
                        MultistreamStateEvent.NodeDetailsEvent(
                            setOf(QuorumForLabels.QuorumItem(2, UpstreamsConfig.Labels.fromMap(mapOf("test1" to "val1")))),
                        ),
                    ),
                ),
            )

        private fun state(
            status: UpstreamAvailability = UpstreamAvailability.OK,
            methods: Set<String> = setOf("method"),
            subs: Set<String> = setOf("sub"),
            caps: Set<Capability> = setOf(Capability.RPC),
            lowerBounds: Set<LowerBoundData> = setOf(LowerBoundData(55, 55, LowerBoundType.STATE)),
            finalizationData: Set<FinalizationData> = setOf(FinalizationData(20, FinalizationType.SAFE_BLOCK)),
            quorumForLabels: Set<QuorumForLabels.QuorumItem> = setOf(QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(mapOf("test" to "val")))),
        ): MultistreamState.CurrentMultistreamState {
            return MultistreamState.CurrentMultistreamState(
                status,
                methods,
                subs,
                caps,
                lowerBounds,
                finalizationData,
                quorumForLabels,
            )
        }
    }
}
