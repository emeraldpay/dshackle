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
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class Describe(
    @Autowired private val multistreamHolder: MultistreamHolder,
    @Autowired private val subscribeStatus: SubscribeStatus,
) {

    fun describe(requestMono: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        return requestMono.map { _ ->
            val resp = BlockchainOuterClass.DescribeResponse.newBuilder()
            resp.buildInfoBuilder.version = Global.version
            multistreamHolder.getAvailable().forEach { chain ->
                multistreamHolder.getUpstream(chain).let { chainUpstreams ->
                    val status = subscribeStatus.chainStatus(chain, chainUpstreams.getStatus(), chainUpstreams)
                    val targets = chainUpstreams.getMethods().getSupportedMethods()
                    val capabilities: MutableSet<Capability> = mutableSetOf()
                    val chainDescription = BlockchainOuterClass.DescribeChain.newBuilder()
                        .setChain(Common.ChainRef.forNumber(chain.id))
                        .addAllSupportedMethods(targets)
                        .addAllSupportedSubscriptions(chainUpstreams.getEgressSubscription().getAvailableTopics())
                        .setStatus(status)
                        .setCurrentHeight(chainUpstreams.getHead().getCurrentHeight() ?: 0)
                        .setCurrentLowerBlock(chainUpstreams.getLowerBlock().blockNumber)
                        .setCurrentLowerSlot(chainUpstreams.getLowerBlock().slot ?: 0)
                    chainUpstreams.getQuorumLabels()
                        .forEach { node ->
                            val nodeDetails = BlockchainOuterClass.NodeDetails.newBuilder()
                                .setQuorum(node.quorum)
                                .addAllLabels(
                                    node.labels.entries.map { label ->
                                        BlockchainOuterClass.Label.newBuilder()
                                            .setName(label.key)
                                            .setValue(label.value)
                                            .build()
                                    },
                                )
                            chainDescription.addNodes(nodeDetails)
                        }
                    capabilities.addAll(chainUpstreams.getCapabilities())
                    chainDescription.addAllCapabilities(
                        capabilities.map {
                            when (it) {
                                Capability.RPC -> BlockchainOuterClass.Capabilities.CAP_CALLS
                                Capability.BALANCE -> BlockchainOuterClass.Capabilities.CAP_BALANCE
                                Capability.WS_HEAD -> BlockchainOuterClass.Capabilities.CAP_WS_HEAD
                            }
                        },
                    )
                    resp.addChains(chainDescription.build())
                }
            }
            resp.build()
        }
    }
}
