package io.emeraldpay.dshackle.quorum

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcError
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcException
import io.emeraldpay.dshackle.upstream.signature.ResponseSigner

class NotNullQuorum : CallQuorum {
    private var sig: ResponseSigner.Signature? = null
    private var result: ByteArray? = null
    private var rpcError: JsonRpcError? = null
    private val resolvers = ArrayList<Upstream>()
    private var allFailed = true
    private val seenUpstreams = HashSet<String>() // just to prevent calling retry upstreams in FilteredApis

    override fun isResolved(): Boolean = result != null

    override fun isFailed(): Boolean = rpcError != null

    override fun record(
        response: ByteArray,
        signature: ResponseSigner.Signature?,
        upstream: Upstream,
    ): Boolean {
        allFailed = false
        val receivedNull = response.isEmpty() || Global.nullValue.contentEquals(response)
        val upId = upstream.getId()
        if (seenUpstreams.contains(upId) || !receivedNull) {
            sig = signature
            result = response
            resolvers.add(upstream)
            return true
        }
        seenUpstreams.add(upId)
        return false
    }

    override fun record(error: JsonRpcException, signature: ResponseSigner.Signature?, upstream: Upstream) {
        val upId = upstream.getId()
        if (seenUpstreams.contains(upId)) {
            if (allFailed) {
                rpcError = error.error
            } else {
                result = Global.nullValue
            }
            sig = signature
        }
        resolvers.add(upstream)
        seenUpstreams.add(upId)
    }

    override fun getSignature(): ResponseSigner.Signature? = sig

    override fun getResult(): ByteArray? = result

    override fun getError(): JsonRpcError? = rpcError

    override fun getResolvedBy(): Collection<Upstream> = resolvers

    override fun toString(): String {
        return "Quorum: Not null"
    }
}
