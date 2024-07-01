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
import java.util.Collections
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Summary details over few upstream nodes. Provides aggregate quorum for nodes with particular label
 */
class QuorumForLabels() {

    private val lock = ReentrantReadWriteLock()
    private val nodes = ArrayList<QuorumItem>()

    constructor(node: QuorumItem) : this() {
        add(node)
    }

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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is QuorumForLabels) return false

        if (nodes != other.nodes) return false

        return true
    }

    override fun hashCode(): Int {
        return nodes.hashCode()
    }

    /**
     * Details for a single element (upstream, node or aggregation)
     */
    data class QuorumItem(val quorum: Int, val labels: UpstreamsConfig.Labels) {
        companion object {
            fun empty(): QuorumItem {
                return QuorumItem(0, UpstreamsConfig.Labels())
            }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is QuorumItem) return false

            if (quorum != other.quorum) return false
            if (labels != other.labels) return false

            return true
        }

        override fun hashCode(): Int {
            var result = quorum
            result = 31 * result + labels.hashCode()
            return result
        }
    }
}
