/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ethereum.EthereumMultistream
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ConnectSyncing(
    private val upstream: EthereumMultistream,
) : SubscriptionConnect<Boolean> {
    companion object {
        private val log = LoggerFactory.getLogger(ConnectSyncing::class.java)
    }

    private var connected: Flux<Boolean>? = null
    private val connectLock = ReentrantLock()

    override fun connect(): Flux<Boolean> {
        val current = connected
        if (current != null) {
            return current
        }
        connectLock.withLock {
            val currentRecheck = connected
            if (currentRecheck != null) {
                return currentRecheck
            }
            val created =
                upstream
                    .observeStatus()
                    .map { it != UpstreamAvailability.OK }
                    .publish()
                    .refCount(1, Duration.ofSeconds(60))
                    .doFinally {
                        // forget it on disconnect, so next time it's recreated
                        connected = null
                    }
            connected = created
            return created
        }
    }
}
