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

    fun subscribe(req: Mono<SubscribeNodeStatusRequest>): Flux<NodeStatusResponse> =
        req.flatMapMany {
            val knownUpstreams = ConcurrentHashMap<String, Boolean>()
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
                            knownUpstreams[up.getId()] = true
                            subscribeUpstreamUpdates(ms.chain, up)
                        }
                    }
            )

            // subscribe on head/status updates for just added upstreams
            val muliStreamUpdates = Flux.merge(
                multistreams.all()
                    .map { ms ->
                        ms.subscribeAddedUpstreams()
                            .distinctUntilChanged {
                                it.getId()
                            }
                            .filter { knownUpstreams[it.getId()] != true }.flatMap {
                                knownUpstreams[it.getId()] = true
                                Flux.concat(
                                    Mono.just(
                                        NodeStatusResponse.newBuilder()
                                            .setNodeId(it.getId())
                                            .setDescription(buildDescription(ms.chain, it))
                                            .setStatus(buildStatus(it.getStatus(), it.getHead().getCurrentHeight()))
                                            .build()
                                    ),
                                    subscribeUpstreamUpdates(ms.chain, it)
                                )
                            }
                    }
            )

            Flux.concat(descriptions, Flux.merge(upstreamUpdates, muliStreamUpdates))
        }

    private fun subscribeUpstreamUpdates(chain: Chain, upstream: Upstream): Flux<NodeStatusResponse> {
        val r = Sinks.many().multicast().directBestEffort<Boolean>()
        val heads = Mono.just(upstream).repeatWhen { r.asFlux() }.sample(Duration.ofSeconds(10)).flatMap { up ->
            up.getHead().getFlux().map { block ->
                NodeStatusResponse.newBuilder()
                    .setNodeId(up.getId())
                    .setStatus(buildStatus(up.getStatus(), block.height))
                    .build()
            }.doFinally {
                if (it == SignalType.ON_COMPLETE) {
                    r.tryEmitNext(true)
                }
            }
        }

        val statuses = upstream.observeStatus().map {
            NodeStatusResponse.newBuilder()
                .setNodeId(upstream.getId())
                .setStatus(buildStatus(it, upstream.getHead().getCurrentHeight()))
                .setDescription(buildDescription(chain, upstream))
                .build()
        }
        return Flux.merge(heads.distinctUntilChanged(), statuses.distinctUntilChanged())
    }

    private fun buildDescription(chain: Chain, up: Upstream): NodeDescription.Builder =
        NodeDescription.newBuilder()
            .setChain(Common.ChainRef.forNumber(chain.id))
            .addAllLabels(
                up.getLabels().flatMap { labels ->
                    labels.map {
                        BlockchainOuterClass.Label.newBuilder()
                            .setName(it.key)
                            .setValue(it.value)
                            .build()
                    }
                }
            )
            .addAllSupportedMethods(up.getMethods().getSupportedMethods())

    private fun buildStatus(status: UpstreamAvailability, height: Long?): NodeStatus.Builder =
        NodeStatus.newBuilder()
            .setAvailability(BlockchainOuterClass.AvailabilityEnum.forNumber(status.grpcId))
            .setCurrentHeight(height ?: 0)
}
