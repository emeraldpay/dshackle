package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson

open class AlwaysQuorum: CallQuorum {

    private var resolved = false
    private var result: ByteArray? = null

    override fun init(head: Head<BlockJson<TransactionId>>) {
    }

    override fun isResolved(): Boolean {
        return resolved
    }

    override fun record(response: ByteArray, upstream: Upstream) {
        result = response
        resolved = true
    }

    override fun getResult(): ByteArray? {
        return result
    }
}