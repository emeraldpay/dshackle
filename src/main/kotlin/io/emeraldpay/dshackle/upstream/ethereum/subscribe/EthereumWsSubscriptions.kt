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

import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.UpstreamSubscriptions
import io.emeraldpay.dshackle.upstream.ethereum.EthereumSubscriptionApi
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamSubscriptions
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import org.slf4j.LoggerFactory

class EthereumWsSubscriptions(
    private val conn: WsSubscriptions
): UpstreamSubscriptions, EthereumUpstreamSubscriptions {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumWsSubscriptions::class.java)
    }

    private val pendingTxes = WebsocketPendingTxes(conn)

    override fun <T> get(method: String): SubscriptionConnect<T>? {
        if (method == EthereumSubscriptionApi.METHOD_PENDING_TXES) {
            return pendingTxes as SubscriptionConnect<T>
        }
        return null
    }

    override fun getPendingTxes(): PendingTxesSource? {
        return pendingTxes
    }

}