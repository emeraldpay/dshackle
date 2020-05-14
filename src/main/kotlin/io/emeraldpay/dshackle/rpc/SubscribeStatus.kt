/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        @Autowired private val upstreams: Upstreams
) {

    fun subscribeStatus(requestMono: Mono<BlockchainOuterClass.StatusRequest>): Flux<BlockchainOuterClass.ChainStatus> {
        return requestMono.flatMapMany {
            val ups = upstreams.getAvailable().mapNotNull { chain ->
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
        }.min() ?: UpstreamAvailability.UNAVAILABLE
        val quorum = ups.filter {
            it.getStatus() > UpstreamAvailability.UNAVAILABLE
        }.count()
        return BlockchainOuterClass.ChainStatus.newBuilder()
                .setAvailability(BlockchainOuterClass.AvailabilityEnum.forNumber(available.grpcId))
                .setChain(Common.ChainRef.forNumber(chain.id))
                .setQuorum(quorum)
                .build()
    }

    class ChainSubscription(val chain: Chain, val up: AggregatedUpstream, val avail: UpstreamAvailability)

}