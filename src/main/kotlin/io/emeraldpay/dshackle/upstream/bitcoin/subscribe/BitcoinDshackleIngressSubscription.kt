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

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.grpc.Chain
import java.util.EnumMap

class BitcoinDshackleIngressSubscription(
    private val blockchain: Chain,
    private val conn: ReactorBlockchainGrpc.ReactorBlockchainStub,
) : IngressSubscription {

    private val subscriptions: EnumMap<BitcoinZmqTopic, BitcoinDshackleSubscriptionSource> = EnumMap(BitcoinZmqTopic::class.java)

    override fun getAvailableTopics(): List<String> {
        return subscriptions.keys
            .map { it.id }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> get(topic: String): SubscriptionConnect<T>? {
        val zmqTopic = BitcoinZmqTopic.findById(topic) ?: return null
        return subscriptions[zmqTopic] as SubscriptionConnect<T>?
    }

    fun update(conf: BlockchainOuterClass.DescribeChain) {
        for (topic in BitcoinZmqTopic.values()) {
            if (conf.supportedSubscriptionsList.contains(topic.id)) {
                subscriptions[topic] = BitcoinDshackleSubscriptionSource(blockchain, conn, topic)
            } else {
                subscriptions.remove(topic)
            }
        }
    }
}
