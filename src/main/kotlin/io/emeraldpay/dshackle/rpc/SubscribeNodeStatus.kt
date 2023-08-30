package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.NodeDescription
import io.emeraldpay.api.proto.BlockchainOuterClass.NodeStatus
import io.emeraldpay.api.proto.BlockchainOuterClass.NodeStatusResponse
import io.emeraldpay.api.proto.BlockchainOuterClass.SubscribeNodeStatusRequest
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.grpc.GrpcUpstream
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Service
class SubscribeNodeStatus(
    private val multistreams: CurrentMultistreamHolder
) {

    companion object {
        private val log = LoggerFactory.getLogger(SubscribeNodeStatus::class.java)
    }

    fun subscribe(req: SubscribeNodeStatusRequest): Flux<NodeStatusResponse> {
        val knownUpstreams = ConcurrentHashMap<String, Sinks.Many<Boolean>>()
        // subscribe on head/status updates for known upstreams
        val upstreamUpdates = Flux.merge(
            multistreams.all()
                .flatMap { ms ->
                    ms.getAll().map { up ->
                        knownUpstreams[up.getId()] = Sinks.many().multicast().directBestEffort()
                        subscribeUpstreamUpdates(ms, up, knownUpstreams[up.getId()]!!)
                    }
                }
        )
        // stop removed upstreams update fluxes
        val removals = Flux.merge(
            multistreams.all()
                .map { ms ->
                    ms.subscribeRemovedUpstreams().mapNotNull { up ->
                        knownUpstreams[up.getId()]?.let {
                            val result = it.tryEmitNext(true)
                            if (result.isFailure) {
                                log.warn("Unable to emit event about removal of an upstream - $result")
                            }
                            knownUpstreams.remove(up.getId())
                            NodeStatusResponse.newBuilder()
                                .setNodeId(up.getId())
                                .setDescription(buildDescription(ms, up))
                                .setStatus(
                                    buildStatus(
                                        UpstreamAvailability.UNAVAILABLE,
                                        up.getHead().getCurrentHeight()
                                    )
                                )
                                .build()
                        }
                    }
                }
        )

        // subscribe on head/status updates for just added upstreams
        val adds = Flux.merge(
            multistreams.all()
                .map { ms ->
                    ms.subscribeAddedUpstreams()
                        .distinctUntilChanged {
                            it.getId()
                        }
                        .filter {
                            !knownUpstreams.contains(it.getId())
                        }
                        .flatMap {
                            knownUpstreams[it.getId()] = Sinks.many().multicast().directBestEffort()
                            Flux.concat(
                                Mono.just(
                                    NodeStatusResponse.newBuilder()
                                        .setNodeId(it.getId())
                                        .setDescription(buildDescription(ms, it))
                                        .setStatus(buildStatus(it.getStatus(), it.getHead().getCurrentHeight()))
                                        .build()
                                ),
                                subscribeUpstreamUpdates(ms, it, knownUpstreams[it.getId()]!!)
                            )
                        }
                }
        )
        val updates = Flux.merge(
            multistreams.all().map { ms ->
                ms.subscribeUpdatedUpstreams().map {
                    NodeStatusResponse.newBuilder()
                        .setNodeId(it.getId())
                        .setDescription(buildDescription(ms, it))
                        .setStatus(buildStatus(it.getStatus(), it.getHead().getCurrentHeight()))
                        .build()
                }
            }
        )

        return Flux.merge(upstreamUpdates, adds, removals, updates)
    }

    private fun subscribeUpstreamUpdates(
        ms: Multistream,
        upstream: Upstream,
        cancel: Sinks.Many<Boolean>
    ): Flux<NodeStatusResponse> {
        val heads = upstream.getHead().getFlux()
            .takeUntilOther(cancel.asFlux())
            .sample(Duration.ofMillis(200))
            .map { block ->
                NodeStatusResponse.newBuilder()
                    .setDescription(
                        NodeDescription.newBuilder()
                            .setNodeId(upstream.nodeId().toInt())
                            .setChain(Common.ChainRef.forNumber(ms.chain.id))
                            .build()
                    )
                    .setNodeId(upstream.getId())
                    .setStatus(buildStatus(upstream.getStatus(), block.height))
                    .build()
            }
        val statuses = upstream.observeStatus()
            .takeUntilOther(cancel.asFlux())
            .map {
                NodeStatusResponse.newBuilder()
                    .setNodeId(upstream.getId())
                    .setStatus(buildStatus(it, upstream.getHead().getCurrentHeight()))
                    .setDescription(buildDescription(ms, upstream))
                    .build()
            }
        val currentState = Mono.just(
            NodeStatusResponse.newBuilder()
                .setNodeId(upstream.getId())
                .setDescription(buildDescription(ms, upstream))
                .setStatus(buildStatus(upstream.getStatus(), upstream.getHead().getCurrentHeight()))
                .build()
        )
        return Flux.concat(currentState, Flux.merge(statuses, heads))
    }

    private fun buildDescription(ms: Multistream, up: Upstream): NodeDescription.Builder {
        val builder = NodeDescription.newBuilder()
            .setChain(Common.ChainRef.forNumber(ms.chain.id))
            .setNodeId(up.nodeId().toInt())
            .addAllNodeLabels(
                up.getLabels().map { nodeLabels ->
                    BlockchainOuterClass.NodeLabels.newBuilder()
                        .addAllLabels(
                            nodeLabels.map {
                                BlockchainOuterClass.Label.newBuilder()
                                    .setName(it.key)
                                    .setValue(it.value)
                                    .build()
                            }
                        )
                        .build()
                }
            )
            .addAllSupportedSubscriptions(up.getSubscriptionTopics())
            .addAllSupportedMethods(up.getMethods().getSupportedMethods())
        (up as? GrpcUpstream)?.let {
            it.getBuildInfo().version?.let { version ->
                builder.nodeBuildInfoBuilder.setVersion(version)
            }
        }
        return builder
    }

    private fun buildStatus(status: UpstreamAvailability, height: Long?): NodeStatus.Builder =
        NodeStatus.newBuilder()
            .setAvailability(Common.AvailabilityEnum.forNumber(status.grpcId))
            .setCurrentHeight(height ?: 0)
}
