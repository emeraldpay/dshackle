package io.emeraldpay.dshackle.upstream.state

import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData

sealed class MultistreamStateEvent {
    data class StatusEvent(
        val status: UpstreamAvailability,
    ) : MultistreamStateEvent()

    data class MethodsEvent(
        val methods: Collection<String>,
    ) : MultistreamStateEvent()

    data class SubsEvent(
        val subs: Collection<String>,
    ) : MultistreamStateEvent()

    data class CapabilitiesEvent(
        val caps: Collection<Capability>,
    ) : MultistreamStateEvent()

    data class LowerBoundsEvent(
        val lowerBounds: Collection<LowerBoundData>,
    ) : MultistreamStateEvent()

    data class FinalizationEvent(
        val finalizationData: Collection<FinalizationData>,
    ) : MultistreamStateEvent()

    data class NodeDetailsEvent(
        val details: Collection<QuorumForLabels.QuorumItem>,
    ) : MultistreamStateEvent()
}
