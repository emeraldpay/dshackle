package io.emeraldpay.dshackle

enum class BlockchainType(
    val apiType: ApiType,
) {
    UNKNOWN(ApiType.JSON_RPC),
    BITCOIN(ApiType.JSON_RPC),
    ETHEREUM(ApiType.JSON_RPC),
    STARKNET(ApiType.JSON_RPC),
    POLKADOT(ApiType.JSON_RPC),
    SOLANA(ApiType.JSON_RPC),
    NEAR(ApiType.JSON_RPC),
    ETHEREUM_BEACON_CHAIN(ApiType.REST),
    COSMOS(ApiType.JSON_RPC);
}

enum class ApiType {
    JSON_RPC, REST;
}