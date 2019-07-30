package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.grpc.Chain
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class SubscribeStatus(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val availableChains: AvailableChains
) {

    fun subscribeStatus(requestMono: Mono<BlockchainOuterClass.StatusRequest>): Flux<BlockchainOuterClass.ChainStatus> {
        return requestMono.flatMapMany {
            val ups = availableChains.getAll().mapNotNull { chain ->
                val chainUpstream = upstreams.getUpstream(chain)
                chainUpstream?.observeStatus()?.map { avail ->
                    ChainSubscription(chain, chainUpstream, avail)
                }
            }

            Flux.merge(ups)
                    .map {
                        chainStatus(it.chain, it.up.getAll())
                    }
        }
    }

    fun chainStatus(chain: Chain, ups: List<Upstream>): BlockchainOuterClass.ChainStatus {
        val available = ups.map { u ->
            u.getStatus()
        }.min()!!
        val quorum = ups.filter {
            it.getStatus() > UpstreamAvailability.UNAVAILABLE
        }.count()
        return BlockchainOuterClass.ChainStatus.newBuilder()
                .setAvailability(BlockchainOuterClass.AvailabilityEnum.forNumber(available.grpcId))
                .setChain(Common.ChainRef.forNumber(chain.id))
                .setQuorum(quorum)
                .build()
    }

    class ChainSubscription(val chain: Chain, val up: AggregatedUpstreams, val avail: UpstreamAvailability)

}