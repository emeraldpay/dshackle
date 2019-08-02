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