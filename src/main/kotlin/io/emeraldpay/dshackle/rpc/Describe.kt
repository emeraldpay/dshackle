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
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.grpc.EthereumGrpcUpstream
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class Describe(
        @Autowired private val multistreamHolder: MultistreamHolder,
        @Autowired private val subscribeStatus: SubscribeStatus
) {

    fun describe(requestMono: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        return requestMono.map { _ ->
            val resp = BlockchainOuterClass.DescribeResponse.newBuilder()
            multistreamHolder.getAvailable().forEach { chain ->
                multistreamHolder.getUpstream(chain)?.let { chainUpstreams ->
                    val status = subscribeStatus.chainStatus(chain, chainUpstreams.getAll())
                    val targets = chainUpstreams.getMethods().getSupportedMethods()
                    val chainDescription = BlockchainOuterClass.DescribeChain.newBuilder()
                            .setChain(Common.ChainRef.forNumber(chain.id))
                            .addAllSupportedMethods(targets)
                            .setStatus(status)
                    chainUpstreams.getAll().let { ups ->
                        ups.forEach { up ->
                            val nodes = QuorumForLabels()
                            if (up is EthereumUpstream) {
                                nodes.add(up.node)
                            } else if (up is BitcoinUpstream) {
                                nodes.add(up.node)
                            } else if (up is EthereumGrpcUpstream) {
                                nodes.add(up.getNodes())
                            }
                            nodes.getAll().forEach { node ->
                                val nodeDetails = BlockchainOuterClass.NodeDetails.newBuilder()
                                        .setQuorum(node.quorum)
                                        .addAllLabels(node.labels.entries.map { label ->
                                            BlockchainOuterClass.Label.newBuilder()
                                                    .setName(label.key)
                                                    .setValue(label.value)
                                                    .build()
                                        })
                                chainDescription.addNodes(nodeDetails)
                            }
                        }
                    }
                    resp.addChains(chainDescription.build())
                }
            }
            resp.build()
        }
    }

}