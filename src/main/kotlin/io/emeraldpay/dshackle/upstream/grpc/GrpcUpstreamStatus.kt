/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.grpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import org.slf4j.LoggerFactory
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference

class GrpcUpstreamStatus {

    companion object {
        private val log = LoggerFactory.getLogger(GrpcUpstreamStatus::class.java)
    }

    private val allLabels: AtomicReference<Collection<UpstreamsConfig.Labels>> = AtomicReference(emptyList())
    private val nodes = AtomicReference<QuorumForLabels>(QuorumForLabels())
    private var targets: CallMethods? = null

    fun update(conf: BlockchainOuterClass.DescribeChain) {
        val updateLabels = ArrayList<UpstreamsConfig.Labels>()
        val updateNodes = QuorumForLabels()

        conf.nodesList.forEach { remoteNode ->
            val node = QuorumForLabels.QuorumItem(
                remoteNode.quorum,
                remoteNode.labelsList.let { provided ->
                    val labels = UpstreamsConfig.Labels()
                    provided.forEach {
                        labels[it.name] = it.value
                    }
                    updateLabels.add(labels)
                    labels
                }
            )
            updateNodes.add(node)
        }

        this.nodes.set(updateNodes)
        this.allLabels.set(Collections.unmodifiableCollection(updateLabels))
        this.targets = DirectCallMethods(conf.supportedMethodsList.toSet())
    }

    fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return allLabels.get()
    }

    fun getNodes(): QuorumForLabels {
        return nodes.get()
    }

    fun getCallMethods(): CallMethods {
        return targets ?: throw IllegalStateException("Upstream is not initialized yet")
    }
}
