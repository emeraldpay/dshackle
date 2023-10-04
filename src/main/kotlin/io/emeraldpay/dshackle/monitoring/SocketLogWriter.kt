/**
 * Copyright (c) 2023 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.monitoring

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.commons.RateLimitedAction
import org.slf4j.LoggerFactory
import org.springframework.util.backoff.ExponentialBackOff
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Send the logs to via a TCP socket. I.e., just push events encoded as JSON to the socket.
 * Uses a LogEncoding to separate multiple events in the stream.
 */
class SocketLogWriter<T>(
    val host: String,
    val port: Int,
    category: CurrentLogWriter.Category,
    serializer: LogSerializer<T>?,
    encoding: LogEncoding,
    private val metrics: LogMetrics = LogMetrics.None(),
    private val bufferSize: Int? = null,
) : LogWriter<T>, BufferingLogWriter<T>(serializer, encoding, metrics = metrics, queueLimit = bufferSize ?: DEFAULT_BUFFER_SIZE) {

    companion object {
        private val log = LoggerFactory.getLogger(SocketLogWriter::class.java)

        private const val DEFAULT_BUFFER_SIZE: Int = 5000
    }

    private val reconnecting = Semaphore(1)
    private var currentSink = AtomicReference<Disposable?>(null)
    private val shouldConnect = AtomicBoolean(false)
    private val closeSlow = RateLimitedAction(Duration.ofSeconds(15))
    private val backOffConfig = ExponentialBackOff(100, 2.0)
    private var backOff = backOffConfig.start()

    private val remoteId = "$category at $host:$port"

    override fun start() {
        if (isRunning) {
            return
        }
        onFull = {
            closeSlow.execute {
                log.warn("Closing slow connection to $remoteId. Closed: ${currentSink.get()?.isDisposed}")
                reconnect()
            }
        }
        shouldConnect.set(true)
        reconnect()
    }

    private fun mayReconnect() {
        if (shouldConnect.get()) {
            if (reconnecting.tryAcquire()) {
                val time = backOff.nextBackOff()
                log.info("Reconnecting to $remoteId in ${time}ms")
                Global.control.schedule(
                    {
                        reconnecting.release()
                        reconnect()
                    }, time, TimeUnit.MILLISECONDS
                )
            } else {
                log.debug("Already scheduled for a reconnection to $remoteId. Skipping a new job...")
            }
        }
    }

    private fun setProducingFlux(send: Disposable?) {
        currentSink.updateAndGet { prev ->
            if (prev != null && prev != send) {
                prev.dispose()
            }
            send
        }
    }

    private fun reconnect() {
        setProducingFlux(connect())
    }

    private fun connect(): Disposable? {
        if (!shouldConnect.get()) {
            return null
        }

        try {
            // it uses a plain TCP NIO Socket implementation instead of a Reactor TCP Client because (a) it's much easier
            // to control in this simple situation and (b) because there is not backpressure in this case so a reactive way
            // has no advantage
            val socket = SocketChannel.open(InetSocketAddress(host, port))
            log.info("Connected to logs server $remoteId")
            return Flux.from(readEncodedFromQueue())
                .map {
                    try {
                        socket.write(it)
                    } catch (t: IOException) {
                        // usually it's a connection issue, i.e., a "broken pipe".
                        // anyway, we just propagate it which closes the socket, etc.
                        throw SilentException(t.message ?: t.javaClass.name)
                    }
                }
                .switchOnFirst { t, u ->
                    if (t.isOnNext) {
                        backOff = backOffConfig.start()
                    }
                    u
                }
                .doOnNext { metrics.collected() }
                .doOnError { t -> log.warn("Failed to write to $remoteId. ${t.message}") }
                .doFinally {
                    log.info("Disconnected from logs server $remoteId / ${it.name}")
                    try {
                        socket.close()
                    } catch (t: Throwable) {
                        log.debug("Failed to close connection to $remoteId")
                    }
                    // schedule a reconnection if needed
                    mayReconnect()
                }
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe()
        } catch (t: Throwable) {
            log.error("Failed to connect to $remoteId", t)
            // schedule a reconnection if needed
            mayReconnect()
            return null
        }
    }

    override fun stop() {
        shouldConnect.set(false)
        currentSink.updateAndGet { prev ->
            prev?.dispose()
            null
        }
        super.stop()
    }

    override fun isRunning(): Boolean {
        return currentSink.get()?.isDisposed == false
    }
}
