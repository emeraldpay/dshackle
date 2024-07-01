package io.emeraldpay.dshackle.upstream.state

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.AggregatedCallMethods
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DisabledCallMethods
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.concurrent.ConcurrentHashMap

class MultistreamState(
    private val onMsUpdate: () -> Unit,
) {
    @Volatile
    private var callMethods: DisabledCallMethods? = null

    @Volatile
    private var capabilities: Set<Capability> = emptySet()

    @Volatile
    private var quorumLabels: List<QuorumForLabels.QuorumItem>? = null

    @Volatile
    private var status: UpstreamAvailability = UpstreamAvailability.UNAVAILABLE

    @Volatile
    private var subs: List<String> = emptyList()
    private val lowerBounds = ConcurrentHashMap<LowerBoundType, LowerBoundData>()
    private val finalizationData = ConcurrentHashMap<FinalizationType, FinalizationData>()

    private val stateHandler = MultistreamStateHandler
    private val stateEvents = Sinks.many().multicast().directBestEffort<Collection<MultistreamStateEvent>>()

    fun getCallMethods(): CallMethods? = callMethods

    fun getQuorumLabels(): List<QuorumForLabels.QuorumItem>? = quorumLabels

    fun getLowerBounds(): Collection<LowerBoundData> = HashSet(lowerBounds.values)

    fun getLowerBound(lowerBoundType: LowerBoundType): LowerBoundData? = lowerBounds[lowerBoundType]

    fun getFinalizationData(): Collection<FinalizationData> = HashSet(finalizationData.values)

    fun getCapabilities(): Set<Capability> = capabilities

    fun lowerBoundsToString(): String =
        lowerBounds.entries.joinToString(", ") { "${it.key}=${it.value.lowerBound}" }

    fun getStatus(): UpstreamAvailability {
        return status
    }

    fun updateState(upstreams: List<Upstream>, subs: List<String>) {
        val oldState = CurrentMultistreamState(this)

        val availableUpstreams = upstreams.filter { it.isAvailable() }
        updateMethods(availableUpstreams)
        updateCapabilities(availableUpstreams)
        updateQuorumLabels(availableUpstreams)
        updateUpstreamBounds(availableUpstreams)
        status = if (upstreams.isEmpty()) UpstreamAvailability.UNAVAILABLE else upstreams.minOf { it.getStatus() }
        this.subs = subs

        stateEvents.emitNext(
            stateHandler.compareStates(oldState, CurrentMultistreamState(this)),
        ) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
    }

    fun stateEvents(): Flux<Collection<MultistreamStateEvent>> = stateEvents.asFlux()

    private fun updateMethods(upstreams: List<Upstream>) {
        upstreams.map { it.getMethods() }.let {
            callMethods = if (callMethods == null) {
                DisabledCallMethods(
                    Defaults.multistreamUnavailableMethodDisableDuration,
                    AggregatedCallMethods(it),
                    onMsUpdate,
                )
            } else {
                DisabledCallMethods(
                    onMsUpdate,
                    Defaults.multistreamUnavailableMethodDisableDuration,
                    AggregatedCallMethods(it),
                    callMethods!!.disabledMethods,
                )
            }
        }
    }

    private fun updateCapabilities(upstreams: List<Upstream>) {
        capabilities = if (upstreams.isEmpty()) {
            emptySet()
        } else {
            upstreams.map { up ->
                up.getCapabilities()
            }.let {
                if (it.isNotEmpty()) {
                    it.reduce { acc, curr -> acc + curr }
                } else {
                    emptySet()
                }
            }
        }
    }

    private fun updateQuorumLabels(ups: List<Upstream>) {
        val nodes = QuorumForLabels()
        ups.forEach { up ->
            if (up is DefaultUpstream) {
                nodes.add(up.getQuorumByLabel())
            }
        }
        quorumLabels = nodes.getAll()
    }

    private fun updateUpstreamBounds(upstreams: List<Upstream>) {
        upstreams
            .flatMap { it.getLowerBounds() }
            .groupBy { it.type }
            .forEach { entry ->
                val min = entry.value.minBy { it.lowerBound }
                lowerBounds[entry.key] = min
            }

        upstreams
            .flatMap { it.getFinalizations() }
            .groupBy { it.type }
            .forEach { entry ->
                val max = entry.value.maxBy { it.height }
                finalizationData[entry.key] = max
            }
    }

    data class CurrentMultistreamState(
        val status: UpstreamAvailability,
        val methods: Collection<String>,
        val subs: Collection<String>,
        val caps: Collection<Capability>,
        val lowerBounds: Collection<LowerBoundData>,
        val finalizationData: Collection<FinalizationData>,
        val nodeDetails: Collection<QuorumForLabels.QuorumItem>,
    ) {
        constructor(state: MultistreamState) : this(
            state.getStatus(),
            state.getCallMethods()?.getSupportedMethods() ?: emptySet(),
            state.subs,
            state.getCapabilities(),
            state.getLowerBounds(),
            state.getFinalizationData(),
            state.getQuorumLabels() ?: emptySet(),
        )
    }
}
