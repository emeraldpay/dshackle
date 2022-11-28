package io.emeraldpay.dshackle

enum class BlockchainType {
    BITCOIN, EVM_POW, EVM_POS;

    companion object {
        @JvmStatic
        fun from(chain: Chain): BlockchainType {
            if (chain == Chain.TESTNET_ROPSTEN ||
                chain == Chain.POLYGON ||
                chain == Chain.OPTIMISM ||
                chain == Chain.BSC ||
                chain == Chain.TESTNET_GOERLI ||
                chain == Chain.ETHEREUM
            ) {
                return EVM_POS
            }
            if (chain == Chain.ETHEREUM_CLASSIC ||
                chain == Chain.ARBITRUM ||
                chain == Chain.FANTOM ||
                chain == Chain.RSK ||
                chain == Chain.TESTNET_KOVAN ||
                chain == Chain.TESTNET_MORDEN ||
                chain == Chain.TESTNET_RINKEBY
            ) {
                return EVM_POW
            }
            if (chain == Chain.BITCOIN ||
                chain == Chain.TESTNET_BITCOIN
            ) {
                return BITCOIN
            }
            throw IllegalArgumentException("Unknown type of blockchain: $chain")
        }
    }
}
