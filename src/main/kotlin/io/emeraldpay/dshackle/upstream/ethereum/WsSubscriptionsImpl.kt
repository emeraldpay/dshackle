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

import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class WsSubscriptionsImpl(
    val wsPool: WsConnectionPool,
) : WsSubscriptions {

    companion object {
        private val log = LoggerFactory.getLogger(WsSubscriptionsImpl::class.java)
    }

    private val ids = AtomicLong(1)

    override fun subscribe(method: String): Flux<ByteArray> {
        val subscriptionId = AtomicReference("")
        val conn = wsPool.getConnection()
        val messages = conn.getSubscribeResponses()
            .filter { it.subscriptionId == subscriptionId.get() }
            .filter { it.result != null } // should never happen
            .map { it.result!! }

        return conn.callRpc(JsonRpcRequest("eth_subscribe", listOf(method), ids.incrementAndGet()))
            .flatMapMany {
                if (it.hasError()) {
                    log.warn("Failed to establish ETH Subscription: ${it.error?.message}")
                    Mono.error(JsonRpcException(it.id, it.error!!))
                } else {
                    subscriptionId.set(it.getResultAsProcessedString())
                    messages
                }
            }
    }
}
