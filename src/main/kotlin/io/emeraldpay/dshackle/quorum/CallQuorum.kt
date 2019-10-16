/**
 * Copyright (c) 2019 ETCDEV GmbH
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
package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import reactor.util.function.Tuple2
import java.util.function.BiFunction
import java.util.function.Predicate

interface CallQuorum {

    fun init(head: Head<BlockJson<TransactionRefJson>>)

    fun isResolved(): Boolean
    fun record(response: ByteArray, upstream: Upstream): Boolean
    fun getResult(): ByteArray?

    companion object {
        fun untilResolved(cq: CallQuorum): Predicate<Any> {
            return Predicate { _ ->
                !cq.isResolved()
            }
        }

        fun asReducer(): BiFunction<CallQuorum, Tuple2<ByteArray, Upstream>, CallQuorum> {
            return BiFunction<CallQuorum, Tuple2<ByteArray, Upstream>, CallQuorum> { a, b ->
                a.record(b.t1, b.t2)
                return@BiFunction a
            }
        }
    }
}