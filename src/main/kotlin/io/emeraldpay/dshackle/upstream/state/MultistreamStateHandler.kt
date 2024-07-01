package io.emeraldpay.dshackle.upstream.state

object MultistreamStateHandler {

    fun compareStates(old: MultistreamState.CurrentMultistreamState, new: MultistreamState.CurrentMultistreamState): Collection<MultistreamStateEvent> {
        val events = mutableListOf<MultistreamStateEvent>()

        if (old.status != new.status) {
            events.add(MultistreamStateEvent.StatusEvent(new.status))
        }
        if (old.methods != new.methods) {
            events.add(MultistreamStateEvent.MethodsEvent(new.methods))
        }
        if (old.subs != new.subs) {
            events.add(MultistreamStateEvent.SubsEvent(new.subs))
        }
        if (old.caps != new.caps) {
            events.add(MultistreamStateEvent.CapabilitiesEvent(new.caps))
        }
        if (old.lowerBounds != new.lowerBounds) {
            events.add(MultistreamStateEvent.LowerBoundsEvent(new.lowerBounds))
        }
        if (old.finalizationData != new.finalizationData) {
            events.add(MultistreamStateEvent.FinalizationEvent(new.finalizationData))
        }
        if (old.nodeDetails != new.nodeDetails) {
            events.add(MultistreamStateEvent.NodeDetailsEvent(new.nodeDetails))
        }

        return events
    }
}
