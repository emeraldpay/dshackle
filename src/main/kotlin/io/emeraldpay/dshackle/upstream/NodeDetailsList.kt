/**
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.collections.ArrayList
import kotlin.concurrent.read
import kotlin.concurrent.write

class NodeDetailsList {

    private val lock = ReentrantReadWriteLock()
    private val nodes = ArrayList<NodeDetails>()

    fun add(node: NodeDetails) {
        lock.read {
            val existing = nodes.find { it.labels == node.labels }
            lock.write {
                if (existing != null) {
                    val merged = NodeDetails(existing.quorum + node.quorum, existing.labels)
                    nodes.remove(existing)
                    nodes.add(merged)
                } else {
                    nodes.add(node)
                }
            }
        }
    }

    fun add(nodes: NodeDetailsList) {
        nodes.nodes.forEach { node -> this.add(node) }
    }

    fun getNodes(): List<NodeDetails> {
        return Collections.unmodifiableList(nodes)
    }

    class NodeDetails(val quorum: Int, val labels: UpstreamsConfig.Labels) {
        companion object {
        }
    }

}