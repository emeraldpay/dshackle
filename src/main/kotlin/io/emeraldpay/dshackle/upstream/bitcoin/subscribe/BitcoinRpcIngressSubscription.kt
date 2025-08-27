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

import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import java.io.Serializable
import java.util.EnumMap

class BitcoinRpcIngressSubscription(
    subscriptionList: List<BitcoinSubscriptionConnect<out Serializable>>,
) : IngressSubscription {
    private val subscriptions: EnumMap<BitcoinZmqTopic, BitcoinSubscriptionConnect<out Serializable>> =
        subscriptionList.associateByTo(EnumMap(BitcoinZmqTopic::class.java)) { it.topic }

    override fun getAvailableTopics(): List<String> =
        subscriptions.keys
            .map { it.id }

    @Suppress("UNCHECKED_CAST")
    override fun <T> get(topic: String): SubscriptionConnect<T>? {
        val zmqTopic = BitcoinZmqTopic.findById(topic) ?: throw IllegalArgumentException("Unknown ZeroMQ topic $topic")
        val s = subscriptions[zmqTopic] ?: return null
        return s as SubscriptionConnect<T>
    }
}
