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
package io.emeraldpay.dshackle.upstream.bitcoin.subscribe

import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import reactor.core.publisher.Flux

class BitcoinEgressSubscription(
    val upstream: BitcoinMultistream,
) : EgressSubscription {
    override fun getAvailableTopics(): List<String> =
        upstream.upstreams
            .flatMap {
                it.getIngressSubscription().getAvailableTopics()
            }.distinct()

    override fun subscribe(
        topic: String,
        params: Any?,
    ): Flux<out Any> {
        val subscribes =
            upstream.upstreams.mapNotNull {
                it
                    .getIngressSubscription()
                    .get<ByteArray>(topic)
                    ?.connect()
            }
        if (subscribes.isEmpty()) {
            return Flux.error(IllegalArgumentException("Subscription topic $topic not supported"))
        }
        return Flux.merge(subscribes)
    }
}
