package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.json.BlockJson

open class NonEmptyQuorum(
        jacksonRpcConverter: JacksonRpcConverter,
        val maxTries: Int = 3
): CallQuorum, ValueAwareQuorum<Any>(jacksonRpcConverter, Any::class.java) {

    private var result: ByteArray? = null
    private var tries: Int = 0

    override fun init(head: Head<BlockJson<TransactionId>>) {
    }

    override fun isResolved(): Boolean {
        return result != null || tries >= maxTries
    }

    override fun recordValue(response: ByteArray, responseValue: Any?, upstream: Upstream) {
        tries++
        if (responseValue != null) {
            result = response
        }
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun recordError(response: ByteArray, errorMessage: String?, upstream: Upstream) {
    }

}