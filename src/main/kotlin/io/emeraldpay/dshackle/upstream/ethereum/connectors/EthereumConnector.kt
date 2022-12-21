package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamSubscriptions
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

interface EthereumConnector : Lifecycle {
    fun getHead(): Head

    fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse>

    fun getUpstreamSubscriptions(): EthereumUpstreamSubscriptions
}
