/**
 * Copyright (c) 2022 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.LinkedList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A fork choice that relies on different Priority of upstreams. In the situation when an upstream has a different block
 * compared to an active Upstream with a higher priority, then it considered as a Forked. Note that an upstream is trusted
 * if it's active, i.e. it's status is good (not in syncing mode, etc). The top priority upstream is always right.
 */
class PriorityForkChoice : ForkChoice {
    companion object {
        private val log = LoggerFactory.getLogger(PriorityForkChoice::class.java)
    }

    /**
     * It doesn't seem to be necessary to keep a long history, because it always sticks to a healthy high priority upstream,
     * and any lagging upstreams is considered as unhealthy anyway.
     */
    private val trackLimit = 24
    private val journalLimit = trackLimit * 2

    private val journal = LinkedList<Pair<BlockId, BlockId>>()
    private val journalLock = ReentrantReadWriteLock()

    private val upstreams = mutableListOf<UpstreamPriority>()
    private val priorities = mutableMapOf<String, Int>()

    private val upstreamBlockLock = ReentrantReadWriteLock()
    private val upstreamBlocks = mutableMapOf<String, List<BlockId>>()

    fun addUpstream(
        upstream: Upstream,
        priority: Int,
    ) {
        // it supposed to be added once on launch so all the following reads should be thread safe
        synchronized(this) {
            val existing = upstreams.find { it.priority == priority }
            if (existing != null) {
                if (existing.upstream.getId() == upstream.getId()) {
                    log.warn("Configuring the same upstream multiple times: ${upstream.getId()}")
                    return
                }
                log.warn(
                    "Two upstreams share the same priority. It can cause an unpredicted behaviour of the Dshackle. " +
                        "Check config for ${existing.upstream.getId()} and ${upstream.getId()}, both have $priority priority",
                )
            }
            priorities[upstream.getId()] = priority
            upstreams.add(UpstreamPriority(priority, upstream))
        }
    }

    fun addUpstream(upstream: Upstream) {
        addUpstream(upstream, upstream.getOptions().priority)
    }

    /**
     * Must be called before actual use. It subscribes to the new upstreams and updates their priorities for the following checks.
     */
    fun followUpstreams(upstreams: Publisher<Upstream>) {
        Flux
            .from(upstreams)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(::addUpstream)
    }

    override fun submit(
        block: BlockContainer,
        upstream: Upstream,
    ): ForkChoice.Status {
        requireNotNull(block.parentHash) { "Block parentHash is null" }
        keepJournal(block)
        val history = getUpstreamBlocks(upstream)
        val head = merge(history, block.parentHash, block.hash)
        setUpstreamBlocks(upstream, head)
        val preferred = findPreferentialUpstream(upstream)
        return if (preferred == null) {
            ForkChoice.Status.NEW
        } else {
            val preferredHistory = getUpstreamBlocks(preferred)
            compareHistory(preferredHistory, head)
        }
    }

    override fun getName(): String = "Priority"

    // ----------- INTERNAL LOGIC

    /**
     * Get the best available upstream after the current. Returns null if the specified is the top priority.
     */
    fun findPreferentialUpstream(current: Upstream): Upstream? {
        val priority = priorities[current.getId()] ?: throw IllegalStateException("Unknown upstream ${current.getId()}")
        return upstreams
            .filter { it.priority > priority }
            .filter { it.upstream.getStatus().isBetterTo(UpstreamAvailability.IMMATURE) }
            .maxByOrNull { it.priority }
            ?.upstream
    }

    fun getUpstreamBlocks(upstream: Upstream): List<BlockId> =
        upstreamBlockLock.read {
            upstreamBlocks[upstream.getId()] ?: emptyList()
        }

    fun setUpstreamBlocks(
        upstream: Upstream,
        current: List<BlockId>,
    ) {
        upstreamBlockLock.write {
            upstreamBlocks[upstream.getId()] = current
        }
    }

    /**
     * Remember relation between blocks. For future checking on the forks.
     */
    fun keepJournal(
        parent: BlockId,
        current: BlockId,
    ): Boolean =
        journalLock.write {
            val pair = Pair(parent, current)
            val exists =
                journal.descendingIterator().asSequence().any {
                    it.first == pair.first && it.second == pair.second
                }
            if (!exists) {
                journal.offer(pair)
                if (journal.size > journalLimit) {
                    journal.poll()
                }
            }
            !exists
        }

    /**
     * Remember the block relation to others.
     */
    fun keepJournal(block: BlockContainer): Boolean {
        requireNotNull(block.parentHash) { "Block parentHash is null" }
        return keepJournal(block.parentHash, block.hash)
    }

    /**
     * Try to find a parent block for the specified.
     */
    fun findParentInJournal(block: BlockId): Pair<BlockId, BlockId>? =
        journalLock.read {
            journal.descendingIterator().asSequence().find {
                it.second == block
            }
        }

    /**
     * Try to rebuild the whole history of the blocks by filling gaps between known information
     */
    fun rebuild(
        existing: List<BlockId>,
        parent: BlockId,
        current: BlockId,
    ): List<BlockId> {
        val result = LinkedList<BlockId>()
        result.add(parent)
        result.add(current)
        while ((existing.isEmpty() || existing.last() != result.first) && result.size < trackLimit) {
            val foundParent = findParentInJournal(result.first) ?: break
            result.addFirst(foundParent.first)
        }
        if (existing.isNotEmpty() && existing.last() == result.first) {
            return existing + result.drop(1)
        }
        return result
    }

    /**
     * Merge the block into the existing with possibly forking from one of the existing blocks.
     * Produces the actual history of blocks, and throws out any existing if it was just replaced by a fork.
     */
    fun merge(
        existing: List<BlockId>,
        parent: BlockId,
        current: BlockId,
    ): List<BlockId> {
        if (existing.isEmpty()) {
            return rebuild(existing, parent, current)
        }
        val parentPos = existing.indexOf(parent)
        if (parentPos < 0) {
            return rebuild(existing, parent, current)
        }
        val start = (parentPos - trackLimit).coerceAtLeast(0)
        val result = ArrayList<BlockId>(parentPos - start + 1)
        result.addAll(existing.subList(start, parentPos + 1))
        result.add(current)
        return result
    }

    /**
     * Compare [uptream's] history to the recognized (i.e, a top priority) history.
     */
    fun compareHistory(
        recognized: List<BlockId>,
        current: List<BlockId>,
    ): ForkChoice.Status {
        if (recognized.isEmpty()) {
            return ForkChoice.Status.NEW
        }
        if (current.isEmpty()) {
            log.warn("Upstream block history is empty")
            return ForkChoice.Status.REJECTED
        }
        if (recognized.last() == current.last()) {
            return ForkChoice.Status.EQUAL
        }
        if (current.any { recognized.last() == it }) {
            return ForkChoice.Status.OUTRUN
        }
        if (recognized.any { current.last() == it }) {
            return ForkChoice.Status.FALLBEHIND
        }
        return ForkChoice.Status.REJECTED
    }

    data class UpstreamPriority(
        val priority: Int,
        val upstream: Upstream,
    )
}
