package io.emeraldpay.dshackle

enum class BlockchainType {
    BITCOIN, EVM_POW, EVM_POS, STARKNET;

    companion object {

        val pow = setOf(
            Chain.ETHEREUM_CLASSIC__MAINNET,
        )
        val bitcoin = setOf(Chain.BITCOIN__MAINNET, Chain.BITCOIN__TESTNET)

        val starknet = setOf(Chain.STARKNET__MAINNET, Chain.STARKNET__TESTNET, Chain.STARKNET__TESTNET_2)

        @JvmStatic
        fun from(chain: Chain): BlockchainType {
            return if (pow.contains(chain)) {
                EVM_POW
            } else if (bitcoin.contains(chain)) {
                BITCOIN
            } else if (starknet.contains(chain)) {
                STARKNET
            } else {
                EVM_POS
            }
        }
    }
}
