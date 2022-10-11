package io.emeraldpay.dshackle.commons

import java.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.BackOffExecution
import org.springframework.util.backoff.ExponentialBackOff
import org.springframework.util.backoff.FixedBackOff
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * A flux holder that reconnects to it on failure taking into account a back off strategy
 */
class DurableFlux<T>(
    private val provider: () -> Flux<T>,
    private val errorBackOff: BackOff,
    private val log: Logger,
) {

    companion object {
        private val defaultLog = LoggerFactory.getLogger(DurableFlux::class.java)

        @JvmStatic
        fun newBuilder(): Builder<*> {
            return Builder<Any>()
        }
    }

    private var messagesSinceStart = 0
    private var errorBackOffExecution = errorBackOff.start()

    fun connect(): Flux<T> {
        return provider.invoke()
            .doOnNext {
                if (messagesSinceStart == 0) {
                    errorBackOffExecution = errorBackOff.start()
                }
                messagesSinceStart++
            }
            .doOnSubscribe {
                messagesSinceStart = 0
            }
            .onErrorResume { t ->
                val backoff = errorBackOffExecution.nextBackOff()
                if (backoff != BackOffExecution.STOP) {
                    log.warn("Connection closed with ${t.message}. Reconnecting in ${backoff}ms")
                    connect().delaySubscription(Duration.ofMillis(backoff))
                } else {
                    log.warn("Connection closed with ${t.message}. Not reconnecting")
                    Mono.error(t)
                }
            }
    }

    class Builder<T> {

        private var provider: (() -> Flux<T>)? = null

        protected var errorBackOff: BackOff = FixedBackOff(1_000, Long.MAX_VALUE)
        protected var log: Logger = DurableFlux.defaultLog

        @Suppress("UNCHECKED_CAST")
        fun <X> using(provider: () -> Flux<X>): Builder<X> {
            this.provider = provider as () -> Flux<T>
            return this as Builder<X>
        }

        fun backoffOnError(time: Duration): Builder<T> {
            errorBackOff = FixedBackOff(time.toMillis(), Long.MAX_VALUE)
            return this
        }

        fun backoffOnError(time: Duration, multiplier: Double, max: Duration? = null): Builder<T> {
            errorBackOff = ExponentialBackOff(time.toMillis(), multiplier).also {
                if (max != null) {
                    it.maxInterval = max.toMillis()
                }
            }
            return this
        }

        fun backoffOnError(backOff: BackOff): Builder<T> {
            errorBackOff = backOff
            return this
        }

        fun logTo(log: Logger): Builder<T> {
            this.log = log
            return this
        }

        fun build(): DurableFlux<T> {
            if (provider == null) {
                throw IllegalStateException("No provider for original Flux")
            }
            return DurableFlux(provider!!,errorBackOff, log)
        }
    }

}