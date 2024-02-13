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
package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import reactor.core.publisher.Flux
import java.time.Duration

class AggregatedPendingTxes(
    private val sources: List<PendingTxesSource>,
) : PendingTxesSource {

    private val track = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(30))
        .maximumSize(10_000)
        .build<TransactionId, Boolean>()

    override fun connect(matcher: Selector.Matcher): Flux<TransactionId> {
        return Flux.merge(
            sources.map { it.connect(matcher) },
        ).filter {
            val res = track.getIfPresent(it)
            if (res == null) {
                track.put(it, true)
                return@filter true
            }
            return@filter false
        }
    }
}
