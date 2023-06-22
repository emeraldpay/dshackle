package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass.BalanceRequest
import java.util.Locale

fun BalanceRequest.getAnyAssetChain(): Chain = when {
    hasAsset() -> Chain.byId(asset.chainValue)
    hasErc20Asset() -> Chain.byId(erc20Asset.chainValue)
    else -> throw IllegalArgumentException("Neither asset nor erc20Asset is specified")
}

fun BalanceRequest.getAssetCodeOrContractAddress(): String = when {
    hasAsset() -> asset.code.lowercase(Locale.getDefault())
    hasErc20Asset() -> erc20Asset.contractAddress.lowercase(Locale.getDefault())
    else -> throw IllegalArgumentException("Neither asset nor erc20Asset is specified")
}
