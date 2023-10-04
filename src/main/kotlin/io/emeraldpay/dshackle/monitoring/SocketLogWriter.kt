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
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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
    bufferSize: Int? = null,
) : LogWriter<T>, BufferingLogWriter<T>(serializer, encoding, metrics = metrics, queueLimit = bufferSize ?: DEFAULT_BUFFER_SIZE) {

    companion object {
        private val log = LoggerFactory.getLogger(SocketLogWriter::class.java)

        private const val DEFAULT_BUFFER_SIZE: Int = 5000
    }

    private val stopped = AtomicBoolean(true)
    private val reconnecting = Semaphore(1)
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
                log.warn("Closing slow connection to $remoteId")
                reconnect()
            }
        }
        stopped.set(false)
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

    private fun reconnect() {
        val thread = connect() ?: return
        thread.start()
    }

    private fun connect(): Thread? {
        if (!shouldConnect.get()) {
            return null
        }

        val runnable = Runnable {
            var socket: SocketChannel? = null
            try {
                // it uses a plain TCP NIO Socket implementation instead of a Reactor TCP Client because (a) it's much easier
                // to control in this simple situation and (b) because there is no backpressure in this case so a reactive way
                // has no advantage
                socket = SocketChannel.open(InetSocketAddress(host, port))
                log.info("Connected to logs server $remoteId")
                sendToConnectedSocket(socket)
            } catch (t: Throwable) {
                log.error("Failed to connect to $remoteId", t)
                // schedule a reconnection if needed
                mayReconnect()
            } finally {
                socket?.close()
            }
        }
        return Thread(runnable, "SocketLogWriter $remoteId").also {
            it.isDaemon = true
        }
    }

    private fun sendToConnectedSocket(socket: SocketChannel) {
        var sentFirst = false

        while (shouldConnect.get() && !stopped.get()) {
            val toSend = next(100)
            if (toSend.isEmpty()) {
                Thread.sleep(25)
                continue
            }
            var pos = 0
            try {
                toSend
                    .forEach {
                        if (stopped.get()) {
                            log.debug("Stopping socket write to $remoteId")
                            returnBack(pos, toSend)
                            return
                        }
                        try {
                            pos++
                            val encoded = encode(it) ?: return@forEach
                            socket.write(encoded)
                            metrics.collected()
                            if (!sentFirst) {
                                sentFirst = true
                                backOff = backOffConfig.start()
                            }
                        } catch (t: IOException) {
                            // usually it's a connection issue, i.e., a "broken pipe".
                            // anyway, we just propagate it which closes the socket, etc.
                            throw SilentException(t.message ?: t.javaClass.name)
                        }
                    }
            } catch (t: Throwable) {
                log.warn("Failed to write to $remoteId. ${t.message}")
                returnBack(pos, toSend)
                // schedule a reconnection if needed
                mayReconnect()
                break
            }
        }
    }

    override fun stop() {
        shouldConnect.set(false)
        stopped.set(true)
        super.stop()
    }

    override fun isRunning(): Boolean {
        return !stopped.get()
    }
}
