package io.emeraldpay.dshackle.commons

import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A flux holder that that creates it only if requested. Keeps it for the following calls, so all the following calls will
 * reuse it. Forgets as soon as it completes/cancelled, so it will be recreated again if needed.
 */
class SharedFluxHolder<T>(
    /**
     * Provider for the flux. Note that it can be called multiple times but only one is used at the same time.
     * I.e., if there is a few calls because of a thread-race only one is kept.
     * But once it's completed a new one may be created if requested.
     */
    private val provider: () -> Flux<T>,
) {

    companion object {
        private val log = LoggerFactory.getLogger(SharedFluxHolder::class.java)
    }

    private val ids = AtomicLong()
    private val lock = ReentrantReadWriteLock()
    private var current: Holder<T>? = null

    fun get(): Flux<T> {
        lock.read {
            if (current != null) {
                return current!!.flux
            }
        }
        // The following doesn't consume resources because it's just create a Flux without actual subscription
        // So even for the case of a thread race it's okay to create many. B/c only one is going to be kept as `current` and subscribed
        val id = ids.incrementAndGet()
        val created = Holder(
            provider.invoke()
                .share()
                .doOnError {
                    log.warn("Shared flux error", it)
                }
                .doFinally {
                    log.warn("Shared flux finished {}", it)
                    onClose(id)
                },
            id,
        )
        lock.write {
            if (current != null) {
                return current!!.flux
            }
            current = created
        }
        return created.flux
    }

    private fun onClose(id: Long) {
        lock.write {
            if (current?.id == id) {
                current = null
            }
        }
    }

    data class Holder<T>(
        val flux: Flux<T>,
        val id: Long,
    )
}
