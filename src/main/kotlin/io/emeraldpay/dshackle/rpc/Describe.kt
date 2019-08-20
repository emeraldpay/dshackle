package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class Describe(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val subscribeStatus: SubscribeStatus
) {

    fun describe(requestMono: Mono<BlockchainOuterClass.DescribeRequest>): Mono<BlockchainOuterClass.DescribeResponse> {
        return requestMono.map { _ ->
            val resp = BlockchainOuterClass.DescribeResponse.newBuilder()
            upstreams.getAvailable().forEach { chain ->
                upstreams.getUpstream(chain)?.let { chainUpstreams ->
                    val status = subscribeStatus.chainStatus(chain, chainUpstreams.getAll())
                    val targets = chainUpstreams.getSupportedTargets()
                    val chainDescription = BlockchainOuterClass.DescribeChain.newBuilder()
                            .setChain(Common.ChainRef.forNumber(chain.id))
                            .addAllSupportedMethods(targets)
                            .setStatus(status)
                    chainUpstreams.getAll().let { ups ->
                        ups.forEach { up ->
                            val nodes = NodeDetailsList()
                            if (up is EthereumUpstream) {
                                nodes.add(up.node)
                            } else if (up is GrpcUpstream) {
                                nodes.add(up.getNodes())
                            }
                            nodes.getNodes().forEach { node ->
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