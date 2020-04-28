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
package io.emeraldpay.dshackle.startup

import io.emeraldpay.dshackle.config.UpstreamsConfig
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.collections.ArrayList
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Summary details over few upstream nodes. Provides aggregate quorum for nodes with particular label
 */
class QuorumForLabels {

    private val lock = ReentrantReadWriteLock()
    private val nodes = ArrayList<QuorumItem>()

    fun add(node: QuorumItem) {
        lock.read {
            val existing = nodes.find { it.labels == node.labels }
            lock.write {
                if (existing != null) {
                    val merged = QuorumItem(existing.quorum + node.quorum, existing.labels)
                    nodes.remove(existing)
                    nodes.add(merged)
                } else {
                    nodes.add(node)
                }
            }
        }
    }

    fun add(nodes: QuorumForLabels) {
        nodes.nodes.forEach { node -> this.add(node) }
    }

    fun getAll(): List<QuorumItem> {
        return Collections.unmodifiableList(nodes)
    }

    /**
     * Details for a single element (upstream, node or aggregation)
     */
    class QuorumItem(val quorum: Int, val labels: UpstreamsConfig.Labels) {
        companion object {
        }
    }

}