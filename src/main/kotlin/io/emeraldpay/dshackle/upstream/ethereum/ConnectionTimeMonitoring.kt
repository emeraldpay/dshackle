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
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * A consumer for a connection events that records the connection time metrics
 */
class ConnectionTimeMonitoring(
    val metrics: RpcMetrics,
) : Consumer<WsConnection.ConnectionStatus> {

    private val connectedAt = AtomicReference<Instant?>(null)

    val connectionTime: Duration?
        get() = connectedAt.get()?.let {
            Duration.between(it, Instant.now()).coerceAtLeast(Duration.ofMillis(0))
        }

    override fun accept(t: WsConnection.ConnectionStatus) {
        if (t == WsConnection.ConnectionStatus.CONNECTED) {
            connectedAt.set(Instant.now())
        } else if (t == WsConnection.ConnectionStatus.DISCONNECTED) {
            connectionTime?.let(metrics::recordConnectionTime)
            connectedAt.set(null)
        }
    }
}
