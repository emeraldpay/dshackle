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
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import org.springframework.util.backoff.BackOffExecution
import org.springframework.util.backoff.ExponentialBackOff
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * A Websocket connection pool which keeps up to `target` connection providing them in a round-robin fashion.
 *
 * It doesn't make all the connections immediately but rather grows them one by one ensuring existing are ok.
 * By default, it adds a new connection every 5 second until it reaches the target number.
 */
class WsConnectionMultiPool(
    private val ethereumWsConnectionFactory: EthereumWsConnectionFactory,
    private val upstream: DefaultUpstream,
    private val connections: Int,
) : WsConnectionPool {

    companion object {
        private const val SCHEDULE_FULL = 60L
        private const val SCHEDULE_GROW = 5L
        private const val SCHEDULE_BROKEN = 15L
    }

    private val current = ArrayList<WsConnection>()
    private var adjustLock = ReentrantReadWriteLock()
    private val index = AtomicInteger(0)
    private var connIndex = 0
    private val connectionInfo = Sinks.many().multicast().directBestEffort<WsConnection.ConnectionInfo>()
    private val connectionSubscriptionMap = mutableMapOf<String, Disposable>()

    var scheduler: ScheduledExecutorService = Global.control

    override fun connect() {
        adjust()
    }

    override fun getConnection(): WsConnection {
        val tries = ExponentialBackOff(50, 1.25).also {
            it.maxElapsedTime = Duration.ofMinutes(1).toMillis()
        }.start()
        var next = next()
        while (next == null) {
            val sleep = tries.nextBackOff()
            if (sleep == BackOffExecution.STOP) {
                throw IllegalStateException("No available WS connection")
            }
            Thread.sleep(sleep)
            next = next()
        }
        return next
    }

    override fun connectionInfoFlux(): Flux<WsConnection.ConnectionInfo> =
        connectionInfo.asFlux()

    override fun close() {
        adjustLock.write {
            connectionSubscriptionMap.values.forEach { it.dispose() }
            connectionSubscriptionMap.clear()
            current.forEach { it.close() }
            current.clear()
        }
    }

    private fun next(): WsConnection? {
        adjustLock.read {
            if (current.isEmpty()) {
                return null
            }
            return current[index.getAndIncrement() % current.size]
        }
    }

    private fun adjust() {
        adjustLock.write {
            // add a new connection only if all existing are active or there are no connections at all
            val allOk = current.all { it.isConnected }
            val schedule: Long
            if (allOk) {
                schedule = if (current.size >= connections) {
                    // recheck the state in a minute and adjust if any connection went bad
                    SCHEDULE_FULL
                } else {
                    current.add(
                        ethereumWsConnectionFactory.createWsConnection(connIndex++) {
                            if (isUnavailable()) {
                                upstream.setStatus(UpstreamAvailability.UNAVAILABLE)
                            }
                        }.also {
                            it.connect()
                            connectionSubscriptionMap[it.connectionId()] = it.connectionInfoFlux().subscribe { info ->
                                connectionInfo.emitNext(info) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                            }
                        },
                    )
                    SCHEDULE_GROW
                }
            } else {
                // technically there is no reason to disconnect because it supposed to reconnect,
                // but to ensure clean start (or other internal state issues) lets completely close all broken and create new
                current.removeIf {
                    if (!it.isConnected) {
                        // DO NOT FORGET to close the connection, otherwise it would keep reconnecting but unused
                        connectionSubscriptionMap.remove(it.connectionId())?.dispose()
                        it.close()
                        true
                    } else {
                        false
                    }
                }
                schedule = SCHEDULE_BROKEN
            }

            scheduler.schedule({ adjust() }, schedule, TimeUnit.SECONDS)
        }
    }

    private fun isUnavailable() = adjustLock.read { current.count { it.isConnected } == 0 }
}
