/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.cache

import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.proto.CachesProto
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.emeraldpay.grpc.Chain
import io.lettuce.core.api.reactive.RedisReactiveCommands
import reactor.core.publisher.Mono

open class ReceiptRedisCache(
    redis: RedisReactiveCommands<String, ByteArray>,
    chain: Chain
) : OnTxRedisCache<ByteArray>(redis, chain, CachesProto.ValueContainer.ValueType.TX_RECEIPT) {

    override fun deserializeValue(value: CachesProto.ValueContainer): ByteArray {
        return value.value.toByteArray()
    }

    override fun serializeValue(value: ByteArray): ByteArray {
        return value
    }

    fun add(json: DefaultContainer<TransactionReceiptJson>): Mono<Void> {
        return super.add(json.txId!!, json.json!!, null, json.height)
    }
}
