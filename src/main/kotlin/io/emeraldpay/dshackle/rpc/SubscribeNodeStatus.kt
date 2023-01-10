package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.BlockchainOuterClass.NodeDescription
import io.emeraldpay.api.proto.BlockchainOuterClass.NodeStatus
import io.emeraldpay.api.proto.BlockchainOuterClass.NodeStatusResponse
import io.emeraldpay.api.proto.BlockchainOuterClass.SubscribeNodeStatusRequest
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.SignalType
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Service
class SubscribeNodeStatus(
    private val multistreams: CurrentMultistreamHolder
) {

    companion object {
        private val RETRY_TIMEOUT = Duration.ofSeconds(10)
        private val log = LoggerFactory.getLogger(SubscribeNodeStatus::class.java)
    }

    fun subscribe(req: Mono<SubscribeNodeStatusRequest>): Flux<NodeStatusResponse> =
        req.flatMapMany {
            val knownUpstreams = ConcurrentHashMap<String, Sinks.Many<Boolean>>()
            // send known upstreams details immediately
            val descriptions = Flux.fromIterable(
                multistreams.all()
                    .flatMap { multiStream ->
                        multiStream.getAll().map { up ->
                            NodeStatusResponse.newBuilder()
                                .setNodeId(up.getId())
                                .setDescription(buildDescription(multiStream.chain, up))
                                .setStatus(buildStatus(up.getStatus(), up.getHead().getCurrentHeight()))
                                .build()
                        }
                    }
            )

            // subscribe on head/status updates for known upstreams
            val upstreamUpdates = Flux.merge(
                multistreams.all()
                    .flatMap { ms ->
                        ms.getAll().map { up ->
                            knownUpstreams[up.getId()] = Sinks.many().multicast().directBestEffort<Boolean>()
                            subscribeUpstreamUpdates(ms.chain, up, knownUpstreams[up.getId()]!!)
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
                                    .setDescription(buildDescription(ms.chain, up))
                                    .setStatus(buildStatus(UpstreamAvailability.UNAVAILABLE, up.getHead().getCurrentHeight()))
                                    .build()
                            }
                        }
                    }
            )

            // subscribe on head/status updates for just added upstreams
            val multiStreamUpdates = Flux.merge(
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
                                knownUpstreams[it.getId()] = Sinks.many().multicast().directBestEffort<Boolean>()
                                Flux.concat(
                                    Mono.just(
                                        NodeStatusResponse.newBuilder()
                                            .setNodeId(it.getId())
                                            .setDescription(buildDescription(ms.chain, it))
                                            .setStatus(buildStatus(it.getStatus(), it.getHead().getCurrentHeight()))
                                            .build()
                                    ),
                                    subscribeUpstreamUpdates(ms.chain, it, knownUpstreams[it.getId()]!!)
                                )
                            }
                    }
            )

            Flux.concat(descriptions, Flux.merge(upstreamUpdates, multiStreamUpdates, removals))
        }

    private fun subscribeUpstreamUpdates(
        chain: Chain,
        upstream: Upstream,
        cancel: Sinks.Many<Boolean>
    ): Flux<NodeStatusResponse> {
        val statuses = upstream.observeStatus()
            .distinctUntilChanged()
            .takeUntilOther(cancel.asFlux())
            .map {
                NodeStatusResponse.newBuilder()
                    .setNodeId(upstream.getId())
                    .setStatus(buildStatus(it, upstream.getHead().getCurrentHeight()))
                    .setDescription(buildDescription(chain, upstream))
                    .build()
            }

        return statuses
    }

    private fun buildDescription(chain: Chain, up: Upstream): NodeDescription.Builder =
        NodeDescription.newBuilder()
            .setChain(Common.ChainRef.forNumber(chain.id))
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
            .addAllSupportedMethods(up.getMethods().getSupportedMethods())

    private fun buildStatus(status: UpstreamAvailability, height: Long?): NodeStatus.Builder =
        NodeStatus.newBuilder()
            .setAvailability(BlockchainOuterClass.AvailabilityEnum.forNumber(status.grpcId))
            .setCurrentHeight(height ?: 0)
}
