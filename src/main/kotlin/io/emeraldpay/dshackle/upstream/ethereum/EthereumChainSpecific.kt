package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.generic.ChainSpecific
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse

object EthereumChainSpecific : ChainSpecific {
    override fun parseBlock(data: JsonRpcResponse, upstreamId: String): BlockContainer {
        return BlockContainer.fromEthereumJson(data.getResult(), upstreamId)
    }

    override fun latestBlockRequest() = JsonRpcRequest("eth_getBlockByNumber", listOf("latest", false))
}
