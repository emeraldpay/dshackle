package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.BlockchainType.STARKNET
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainSpecific
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.starknet.StarknetChainSpecific

interface ChainSpecific {
    fun parseBlock(data: JsonRpcResponse, upstreamId: String): BlockContainer

    fun latestBlockRequest(): JsonRpcRequest
}

object ChainSpecificRegistry {
    fun resolve(chain: Chain): ChainSpecific {
        if (BlockchainType.from(chain) == STARKNET) {
            return StarknetChainSpecific
        }
        return EthereumChainSpecific
    }
}
