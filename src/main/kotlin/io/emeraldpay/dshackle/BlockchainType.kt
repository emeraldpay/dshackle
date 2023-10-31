package io.emeraldpay.dshackle

enum class BlockchainType {
    BITCOIN, ETHEREUM, STARKNET;

    companion object {
        val bitcoin = setOf(Chain.BITCOIN__MAINNET, Chain.BITCOIN__TESTNET)

        val starknet = setOf(Chain.STARKNET__MAINNET, Chain.STARKNET__TESTNET, Chain.STARKNET__TESTNET_2)

        @JvmStatic
        fun from(chain: Chain): BlockchainType {
            return if (bitcoin.contains(chain)) {
                BITCOIN
            } else if (starknet.contains(chain)) {
                STARKNET
            } else {
                ETHEREUM
            }
        }
    }
}
