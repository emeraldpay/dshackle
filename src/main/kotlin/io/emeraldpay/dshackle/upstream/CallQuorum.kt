package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import reactor.util.function.Tuple2
import java.util.function.BiFunction
import java.util.function.Predicate

interface CallQuorum {

    fun init(head: Head<BlockJson<TransactionId>>)

    fun isResolved(): Boolean
    fun record(response: ByteArray, upstream: Upstream)
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