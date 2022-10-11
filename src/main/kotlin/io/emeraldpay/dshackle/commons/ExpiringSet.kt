package io.emeraldpay.dshackle.commons

import java.time.Duration
import java.util.LinkedList
import java.util.TreeSet
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.apache.commons.collections4.iterators.UnmodifiableIterator
import org.slf4j.LoggerFactory

/**
 * A naive implementation of a Set with a limit for elements and an expiration time. Supposed to be used a filter for uniqueness.
 * Internally it uses a TreeSet and a journal of added elements, which is used ot remove elements when they expire or the list grows too large.
 * It's tread safe, but may be suboptimal to use in multithreaded scenario because of internal locks.
 */
class ExpiringSet<T>(
    ttl: Duration,
    comparator: Comparator<T>,
    val limit: Int,
): MutableSet<T> {

    companion object {
        private val log = LoggerFactory.getLogger(ExpiringSet::class.java)
    }

    private val tree = TreeSet<T>(comparator)
    private val lock = ReentrantLock()
    private val journal = LinkedList<JournalItem<T>>()
    private var count = 0

    private val ttl = ttl.toMillis()

    data class JournalItem<T>(
        val since: Long = System.currentTimeMillis(),
        val value: T
    ) {
        fun isExpired(ttl: Long): Boolean {
            return System.currentTimeMillis() > since + ttl
        }
    }

    override val size: Int
        get() = count

    override fun clear() {
        lock.withLock {
            tree.clear()
            journal.clear()
            count = 0
        }
    }

    override fun addAll(elements: Collection<T>): Boolean {
        var changed = false
        elements.forEach {
            changed = changed || add(it)
        }
        return changed
    }

    override fun add(element: T): Boolean {
        lock.withLock {
            val added = tree.add(element)
            if (added) {
                journal.offer(JournalItem(value = element))
                count++
                shrink()
            }
            return added
        }
    }

    override fun isEmpty(): Boolean {
        return count == 0
    }

    override fun iterator(): MutableIterator<T> {
        // not mutable
        return UnmodifiableIterator.unmodifiableIterator(tree.iterator())
    }

    override fun retainAll(elements: Collection<T>): Boolean {
        lock.withLock {
            var changed = false
            val iter = tree.iterator()
            while (iter.hasNext()) {
                val next = iter.next()
                if (!elements.contains(next)) {
                    changed = true
                    iter.remove()
                    count--
                }
            }
            return changed
        }
    }

    override fun removeAll(elements: Collection<T>): Boolean {
        var changed = false
        elements.forEach {
            changed = changed || remove(it)
        }
        return changed
    }

    override fun remove(element: T): Boolean {
        lock.withLock {
            val changed = tree.remove(element)
            if (changed) {
                count--
            }
            return changed
        }
    }

    override fun containsAll(elements: Collection<T>): Boolean {
        return elements.all { contains(it) }
    }

    override fun contains(element: T): Boolean {
        lock.withLock {
            return tree.contains(element)
        }
    }

    fun shrink() {
        lock.withLock {
            val iter = journal.iterator()
            val removeAtLeast = (count - limit).coerceAtLeast(0)
            var removed = 0
            var stop = false
            while (!stop && iter.hasNext()) {
                val next = iter.next()
                val overflow = removeAtLeast > removed
                val expired = next.isExpired(ttl)
                if (overflow || expired) {
                    iter.remove()
                    if (tree.remove(next.value)) {
                        removed++
                    }
                }
                // we always delete expired elements so don't stop on that
                if (!expired) {
                    // but if we already deleted all non-expired element (i.e., started because it grew too large)
                    // then we stop as soon as we don't have any overflow
                    stop = !overflow
                }
            }
            count -= removed
        }
    }

}