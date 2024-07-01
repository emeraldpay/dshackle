package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.state.MultistreamStateEvent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

@Service
class SubscribeChainStatus(
    private val multistreamHolder: MultistreamHolder,
    private val chainEventMapper: ChainEventMapper,
) {

    companion object {
        private val log = LoggerFactory.getLogger(SubscribeChainStatus::class.java)
    }

    fun chainStatuses(): Flux<BlockchainOuterClass.SubscribeChainStatusResponse> {
        return Flux.merge(
            // we need to track not only multistreams with upstreams but all of them
            // because upstreams can be added in runtime with hot config reload
            multistreamHolder.all()
                .filter { Common.ChainRef.forNumber(it.chain.id) != null }
                .map { ms ->
                    Flux.concat(
                        // the first event must be filled with all fields
                        firstFullEvent(ms),
                        Flux.merge(
                            // head events are separated from others
                            headEvents(ms),
                            multistreamEvents(ms),
                        ),
                    )
                },
        ).doOnError {
            log.error("Error during sending chain statuses", it)
        }
    }

    private fun multistreamEvents(ms: Multistream): Flux<BlockchainOuterClass.SubscribeChainStatusResponse> {
        return ms.stateEvents()
            .filter { it.isNotEmpty() }
            .map { events ->
                val response = BlockchainOuterClass.SubscribeChainStatusResponse.newBuilder()
                val chainDescription = BlockchainOuterClass.ChainDescription.newBuilder()
                    .setChain(Common.ChainRef.forNumber(ms.chain.id))

                events.forEach {
                    chainDescription.addChainEvent(processMsEvent(it))
                }

                response.setChainDescription(chainDescription.build())
                response.build()
            }
    }

    private fun processMsEvent(event: MultistreamStateEvent): BlockchainOuterClass.ChainEvent {
        return when (event) {
            is MultistreamStateEvent.CapabilitiesEvent -> chainEventMapper.mapCapabilities(event.caps)
            is MultistreamStateEvent.FinalizationEvent -> chainEventMapper.mapFinalizationData(event.finalizationData)
            is MultistreamStateEvent.LowerBoundsEvent -> chainEventMapper.mapLowerBounds(event.lowerBounds)
            is MultistreamStateEvent.MethodsEvent -> chainEventMapper.supportedMethods(event.methods)
            is MultistreamStateEvent.NodeDetailsEvent -> chainEventMapper.mapNodeDetails(event.details)
            is MultistreamStateEvent.StatusEvent -> chainEventMapper.chainStatus(event.status)
            is MultistreamStateEvent.SubsEvent -> chainEventMapper.supportedSubs(event.subs)
        }
    }

    private fun firstFullEvent(ms: Multistream): Mono<BlockchainOuterClass.SubscribeChainStatusResponse> {
        return Mono.justOrEmpty(ms.getHead().getCurrent())
            .map { toFullResponse(it!!, ms) }
            .switchIfEmpty {
                // in case if there is still no head we mush wait until we get it
                ms.getHead()
                    .getFlux()
                    .next()
                    .map { toFullResponse(it!!, ms) }
            }
    }

    private fun headEvents(ms: Multistream): Flux<BlockchainOuterClass.SubscribeChainStatusResponse> {
        return ms.getHead()
            .getFlux()
            .skip(1)
            .map {
                BlockchainOuterClass.SubscribeChainStatusResponse.newBuilder()
                    .setChainDescription(
                        BlockchainOuterClass.ChainDescription.newBuilder()
                            .setChain(Common.ChainRef.forNumber(ms.chain.id))
                            .addChainEvent(chainEventMapper.mapHead(it))
                            .build(),
                    )
                    .build()
            }
    }

    private fun toFullResponse(head: BlockContainer, ms: Multistream): BlockchainOuterClass.SubscribeChainStatusResponse {
        return BlockchainOuterClass.SubscribeChainStatusResponse.newBuilder()
            .setChainDescription(
                BlockchainOuterClass.ChainDescription.newBuilder()
                    .setChain(Common.ChainRef.forNumber(ms.chain.id))
                    .addChainEvent(chainEventMapper.chainStatus(ms.getStatus()))
                    .addChainEvent(chainEventMapper.mapHead(head))
                    .addChainEvent(chainEventMapper.supportedMethods(ms.getMethods().getSupportedMethods()))
                    .addChainEvent(chainEventMapper.supportedSubs(ms.getEgressSubscription().getAvailableTopics()))
                    .addChainEvent(chainEventMapper.mapCapabilities(ms.getCapabilities()))
                    .addChainEvent(chainEventMapper.mapLowerBounds(ms.getLowerBounds()))
                    .addChainEvent(chainEventMapper.mapFinalizationData(ms.getFinalizations()))
                    .addChainEvent(chainEventMapper.mapNodeDetails(ms.getQuorumLabels()))
                    .build(),
            )
            .setBuildInfo(
                BlockchainOuterClass.BuildInfo.newBuilder()
                    .setVersion(Global.version)
                    .build(),
            )
            .setFullResponse(true)
            .build()
    }
}
