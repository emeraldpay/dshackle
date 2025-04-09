package io.emeraldpay.dshackle.upstream.calls

import io.emeraldpay.dshackle.quorum.AlwaysQuorum
import io.emeraldpay.dshackle.quorum.BroadcastQuorum
import io.emeraldpay.dshackle.quorum.CallQuorum
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException

class DefaultKadenaMethods : CallMethods {

    private val kadenaMethods = setOf(
        getMethod("/chain/*/hash"),
        postMethod("/chain/*/hash"),
        getMethod("/chain/*/header"),
        getMethod("/chain/*/header/*"),
        postMethod("/chain/*/header/branch"),
        getMethod("/chain/*/payload/*"),
        postMethod("/chain/*/payload/batch"),
        getMethod("/chain/*/payload/*/outputs"),
        postMethod("/chain/*/payload/*/outputs/batch"),
        getMethod("/chain/*/payload/*"),
        postMethod("/chain/*/pact/local"),
        postMethod("/chain/*/pact/send"),
        postMethod("/chain/*/pact/poll"),
        postMethod("/chain/*/pact/listen"),
        postMethod("/chain/*/pact/private"),
        postMethod("/chain/*/pact/spv"),
        postMethod("/chain/*/mempool/getPending"),
        postMethod("/chain/*/mempool/member"),
        postMethod("/chain/*/mempool/lookup"),
    )

    private val insert = setOf(
        putMethod("/chain/*/mempool/insert"),
    )

    private val allowedMethods: Set<String> = kadenaMethods + insert

    override fun createQuorumFor(method: String): CallQuorum {
        if (insert.contains(method)) {
            return BroadcastQuorum()
        }
        return AlwaysQuorum()
    }

    override fun isCallable(method: String): Boolean {
        return allowedMethods.contains(method)
    }

    override fun getSupportedMethods(): Set<String> {
        return allowedMethods.toSortedSet()
    }

    override fun isHardcoded(method: String): Boolean {
        return false
    }

    override fun executeHardcoded(method: String): ByteArray {
        throw RpcException(-32601, "Method not found")
    }

    override fun getGroupMethods(groupName: String): Set<String> {
        return when (groupName) {
            "default" -> getSupportedMethods()
            else -> emptyList()
        }.toSet()
    }

    private fun getMethod(method: String) = "GET#$method"

    private fun postMethod(method: String) = "POST#$method"

    private fun putMethod(method: String) = "PUT#$method"
}
