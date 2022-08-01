package io.emeraldpay.dshackle.data

import java.util.concurrent.atomic.AtomicReference

class RingSet<T>(
    private val maxSize: Int
) : Set<T> {
    private var setRef: AtomicReference<LinkedHashSet<T>> = AtomicReference(LinkedHashSet<T>())
    override val size: Int
        get() = setRef.get().size

    fun add(element: T) {
        setRef.getAndUpdate { set ->
            if (!set.contains(element)) {
                val copyset = LinkedHashSet<T>(set)
                copyset.add(element)
                if (copyset.size > maxSize) {
                    copyset.remove(set.elementAt(0))
                }
                copyset
            } else {
                set
            }
        }
    }

    override fun isEmpty(): Boolean {
        return setRef.get().isEmpty()
    }
    override fun contains(element: @UnsafeVariance T): Boolean {
        return setRef.get().contains(element)
    }
    override fun iterator(): Iterator<T> {
        return setRef.get().iterator()
    }

    override fun containsAll(elements: Collection<@UnsafeVariance T>): Boolean {
        return setRef.get().containsAll(elements)
    }
}
