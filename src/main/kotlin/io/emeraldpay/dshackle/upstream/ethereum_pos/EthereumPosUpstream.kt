package io.emeraldpay.dshackle.upstream.ethereum_pos

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

class EthereumPosUpstream(
    id: String,
    options: UpstreamsConfig.Options,
    role: UpstreamsConfig.UpstreamRole,
    targets: CallMethods?,
    node: QuorumForLabels.QuorumItem?,
    private val ethereumUpstream: EthereumUpstream
) : DefaultUpstream(id, options, role, targets, node) {
    override fun getCapabilities(): Set<Capability> {
        return ethereumUpstream.getCapabilities()
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return ethereumUpstream.getLabels()
    }

    override fun getHead() : Head {
        return ethereumUpstream.getHead()
    }

    override fun isGrpc(): Boolean {
        return false
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return ethereumUpstream.getApi()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }
}