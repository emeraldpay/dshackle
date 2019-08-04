package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.JacksonRpcConverter
import io.infinitape.etherjar.rpc.json.BlockJson

open class BroadcastQuorum(
        jacksonRpcConverter: JacksonRpcConverter,
        val quorum: Int = 3
): CallQuorum, ValueAwareQuorum<String>(jacksonRpcConverter, String::class.java) {

    private var result: ByteArray? = null
    private var txid: String? = null
    private var calls = 0

    override fun init(head: Head<BlockJson<TransactionId>>) {
    }

    override fun isResolved(): Boolean {
        return calls >= quorum
    }

    override fun getResult(): ByteArray? {
        return result
    }

    override fun recordValue(response: ByteArray, responseValue: String?, upstream: Upstream) {
        calls++
        if (txid == null && responseValue != null) {
            txid = responseValue
            result = response
        }
    }

    override fun recordError(response: ByteArray,  errorMessage: String?, upstream: Upstream) {
        // can be "message: known transaction: TXID" or "message: Nonce too low"
        calls++
        if (result == null) {
            result = response
        }
    }

}