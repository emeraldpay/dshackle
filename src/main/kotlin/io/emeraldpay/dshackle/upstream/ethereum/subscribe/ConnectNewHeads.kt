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

import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.ethereum.EthereumLikeMultistream
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.json.NewHeadMessage
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/**
 * Connects/reconnects to the upstream to produce NewHeads messages
 */
class ConnectNewHeads(
    private val upstream: EthereumLikeMultistream,
    private val scheduler: Scheduler,
) : SubscriptionConnect<NewHeadMessage> {

    private val connected: MutableMap<String, Flux<NewHeadMessage>> = ConcurrentHashMap()

    override fun connect(matcher: Selector.Matcher): Flux<NewHeadMessage> =
        connected.computeIfAbsent(matcher.describeInternal()) { key ->
            ProduceNewHeads(upstream.getHead(matcher))
                .start()
                .publishOn(scheduler)
                .publish()
                .refCount(1, Duration.ofSeconds(60))
                .doFinally {
                    // forget it on disconnect, so next time it's recreated
                    connected.remove(key)
                }
        }
}
