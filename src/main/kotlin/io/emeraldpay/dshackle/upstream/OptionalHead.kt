package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.data.BlockContainer
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A Head that can be enabled/disabled at any moment, so it starts to use actual Head (delegate)
 * only when it's enabled
 */
class OptionalHead(
    /**
     * Real Head that would be used when this one is enabled
     */
    private val delegate: Head,
) : Head,
    Lifecycle {
    companion object {
        private val log = LoggerFactory.getLogger(OptionalHead::class.java)
    }

    private val enableLock = ReentrantLock()
    private var enabled = false

    private val currentSubscriptions = ConcurrentLinkedQueue<AtomicBoolean>()
    private val onEnable =
        Sinks
            .many()
            .multicast()
            .directBestEffort<Boolean>()

    /**
     * Lifecycle Flag, used to synchronize the lifecycle with delegate
     */
    private var started = false

    fun setEnabled(enable: Boolean) {
        val changed: Boolean
        enableLock.withLock {
            val prev = enabled
            enabled = enable
            changed = prev != enable
            if (changed && !enable) {
                // stop all current subscriptions
                while (currentSubscriptions.isNotEmpty()) {
                    currentSubscriptions.poll().set(false)
                }
            }
        }
        if (changed) {
            adjustLifecycle()
            onEnable.tryEmitNext(enable)
        }
    }

    /**
     * Synchronise the lifecycle of the delegate and current head. Ensures that if this head is started and enabled then
     * the delegate is also started. Otherwise, stop the delegate
     */
    fun adjustLifecycle() {
        if (delegate !is Lifecycle) {
            return
        }
        val shouldStop: Boolean
        val shouldStart: Boolean
        enableLock.withLock {
            val delegateIsActive = enabled
            shouldStop = !started || !delegateIsActive
            shouldStart = started && delegateIsActive
        }
        if (shouldStart && !delegate.isRunning) {
            delegate.start()
        } else if (shouldStop && delegate.isRunning) {
            delegate.stop()
        }
    }

    override fun getFlux(): Flux<BlockContainer> {
        // a postponed re-subscription to the same method
        val restart =
            Mono
                .fromCallable { log.trace("Restart optional head to $delegate") }
                .flatMapMany { getFlux() }
                .subscribeOn(Schedulers.boundedElastic())
        return if (enabled) {
            Flux.concat(
                getEnabledFlux(),
                restart,
            )
        } else {
            getDisabledFlux()
                .subscribeOn(Schedulers.boundedElastic())
                .thenMany(restart)
        }
    }

    /**
     * A flux for blocks when this head is enabled. In general, it just provides with the flux from the delegate.
     * Which can also be stopped if this head gets disabled.
     */
    protected fun getEnabledFlux(): Flux<BlockContainer> {
        val control = AtomicBoolean(true)
        currentSubscriptions.offer(control)
        return delegate
            .getFlux()
            .subscribeOn(Schedulers.boundedElastic())
            .takeWhile { control.get() }
    }

    /**
     * A mono for blocks when this head is disabled. Basically never produce anything, but completes when this head
     * gets enabled.
     */
    protected fun getDisabledFlux(): Mono<Void> =
        Flux
            .from(onEnable.asFlux())
            .filter { it }
            .next()
            .then()

    override fun onBeforeBlock(handler: Runnable) {
        delegate.onBeforeBlock(handler)
    }

    override fun getCurrentHeight(): Long? {
        if (enabled) {
            return delegate.getCurrentHeight()
        }
        return null
    }

    override fun start() {
        enableLock.withLock {
            started = true
            adjustLifecycle()
        }
    }

    override fun stop() {
        enableLock.withLock {
            started = false
            adjustLifecycle()
        }
    }

    override fun isRunning(): Boolean =
        enableLock.withLock {
            started && (delegate !is Lifecycle || delegate.isRunning)
        }
}
