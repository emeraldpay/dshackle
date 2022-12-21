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

import io.emeraldpay.dshackle.commons.ExpiringSet
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.HexDataComparator
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.time.Duration

class AggregatedPendingTxes(
    private val sources: List<PendingTxesSource>
) : PendingTxesSource {

    companion object {
        private val log = LoggerFactory.getLogger(AggregatedPendingTxes::class.java)
    }

    private val track = ExpiringSet<TransactionId>(
        Duration.ofSeconds(30),
        HexDataComparator() as Comparator<TransactionId>,
        10_000
    )

    override fun connect(matcher: Selector.Matcher): Flux<TransactionId> {
        return Flux.merge(
            sources.map { it.connect(matcher) } // todo check
        ).filter(track::add)
    }
}
