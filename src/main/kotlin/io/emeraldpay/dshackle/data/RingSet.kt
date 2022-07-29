package io.emeraldpay.dshackle.data

import java.util.concurrent.atomic.AtomicReference

class RingSet<T>(
    private val maxSize: Int
): Set<T> {
    private var seqValues: AtomicReference<List<T>> = AtomicReference(emptyList())
    private var set: Set<T> = emptySet()
    override val size: Int
        get() = set.size

    fun add(element: T) {
        if (set.contains(element)) {
            return
        }
        seqValues.getAndUpdate { vals ->
            vals.let {
                if (vals.size > maxSize) {
                    vals.drop(1)
                } else {
                    vals
                }
            }.plus(element).let {
                set = HashSet(it)
                it
            }
        }
    }

    override fun isEmpty(): Boolean {
        return set.isEmpty()
    }
    override fun contains(element: @UnsafeVariance T): Boolean {
        return set.contains(element)
    }
    override fun iterator(): Iterator<T> {
        return set.iterator()
    }

    override fun containsAll(elements: Collection<@UnsafeVariance T>): Boolean {
        return set.containsAll(elements)
    }
}