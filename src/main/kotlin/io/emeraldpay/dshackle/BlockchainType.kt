package io.emeraldpay.dshackle

enum class BlockchainType {
    BITCOIN, EVM_POW, EVM_POS;

    companion object {

        val pow = setOf(
            Chain.ETHEREUM_CLASSIC__MAINNET, Chain.RSK__MAINNET, Chain.ETHEREUM__KOVAN,
            Chain.ETHEREUM__MORDEN, Chain.ETHEREUM__RINKEBY
        )
        val bitcoin = setOf(Chain.BITCOIN__MAINNET, Chain.BITCOIN__TESTNET)
        @JvmStatic
        fun from(chain: Chain): BlockchainType {
            return if (pow.contains(chain)) {
                EVM_POW
            } else if (bitcoin.contains(chain)) {
                BITCOIN
            } else {
                EVM_POS
            }
        }
    }
}
