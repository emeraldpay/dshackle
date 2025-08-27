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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import org.springframework.util.backoff.BackOffExecution
import org.springframework.util.backoff.ExponentialBackOff
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Consumer
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A Websocket connection pool which keeps up to `target` connection providing them in a round-robin fashion.
 *
 * It doesn't make all the connections immediately but rather grows them one by one ensuring existing are ok.
 * By default, it adds a new connection every 5 second until it reaches the target number.
 */
class WsConnectionMultiPool(
    private val factory: EthereumWsFactory,
    val target: Int,
) : WsConnectionPool() {
    companion object {
        private const val SCHEDULE_FULL = 60L
        private const val SCHEDULE_GROW = 5L
        private const val SCHEDULE_BROKEN = 15L
    }

    private val current = ArrayList<WsConnection>()
    private var adjustLock = ReentrantReadWriteLock()
    private val index = AtomicInteger(0)

    var scheduler: ScheduledExecutorService = Global.control

    val internalStatusUpdates =
        Consumer<WsConnection.ConnectionStatus> {
            val connectedCount =
                adjustLock.read {
                    current.count { it.isConnected }
                }
            if (connectedCount == 0) {
                statusUpdates?.accept(UpstreamAvailability.UNAVAILABLE)
            } else {
                statusUpdates?.accept(UpstreamAvailability.OK)
            }
        }

    fun adjust() {
        adjustLock.write {
            // add a new connection only all existing age good connections
            val allOk = current.all { it.isConnected }
            val schedule: Long
            if (allOk) {
                if (current.size >= target) {
                    // recheck the state in a minute and adjust if any connection went bad
                    schedule = SCHEDULE_FULL
                } else {
                    current.add(
                        factory.create(internalStatusUpdates).also {
                            it.connect()
                        },
                    )
                    schedule = SCHEDULE_GROW
                }
            } else {
                // technically there is no reason to disconnect because it supposed to reconnect,
                // but to ensure clean start (or other internal state issues) lets completely close all broken and create new
                current.removeIf {
                    if (!it.isConnected) {
                        // DO NOT FORGET to close the connection, otherwise it would keep reconnecting but unused
                        it.close()
                        true
                    } else {
                        false
                    }
                }
                schedule = SCHEDULE_BROKEN
            }

            scheduler.schedule({ adjust() }, schedule, TimeUnit.SECONDS)

            if (current.isEmpty()) {
                statusUpdates?.accept(UpstreamAvailability.UNAVAILABLE)
            }
        }
    }

    fun next(): WsConnection? {
        adjustLock.read {
            if (current.isEmpty()) {
                return null
            }
            return current[index.getAndIncrement() % current.size]
        }
    }

    override fun connect() {
        adjust()
    }

    override fun getConnection(): WsConnection {
        val tries =
            ExponentialBackOff(50, 1.25)
                .also {
                    it.maxElapsedTime = Duration.ofMinutes(1).toMillis()
                }.start()
        var next = next()
        while (next == null) {
            val sleep = tries.nextBackOff()
            if (sleep == BackOffExecution.STOP) {
                throw IllegalStateException("No available WS connection")
            }
            try {
                Thread.sleep(sleep)
            } catch (t: InterruptedException) {
            }
            next = next()
        }
        return next
    }

    override fun close() {
        adjustLock.write {
            current.forEach { it.close() }
            current.clear()
        }
    }
}
