package io.emeraldpay.dshackle

enum class BlockchainType {
    BITCOIN, EVM_POW, EVM_POS;

    companion object {
        @JvmStatic
        fun from(chain: Chain): BlockchainType {
            if (chain == Chain.ETHEREUM__ROPSTEN ||
                chain == Chain.POLYGON_POS__MAINNET ||
                chain == Chain.OPTIMISM__MAINNET ||
                chain == Chain.ARBITRUM__MAINNET ||
                chain == Chain.BSC__MAINNET ||
                chain == Chain.ETHEREUM__GOERLI ||
                chain == Chain.ETHEREUM__MAINNET ||
                chain == Chain.ETHEREUM__SEPOLIA ||
                chain == Chain.ARBITRUM__GOERLI ||
                chain == Chain.OPTIMISM__GOERLI ||
                chain == Chain.ARBITRUM_NOVA__MAINNET ||
                chain == Chain.POLYGON_ZKEVM__MAINNET ||
                chain == Chain.POLYGON_ZKEVM__TESTNET ||
                chain == Chain.ZKSYNC__MAINNET ||
                chain == Chain.ZKSYNC__TESTNET ||
                chain == Chain.POLYGON_POS__MUMBAI ||
                chain == Chain.BASE__MAINNET ||
                chain == Chain.BASE__GOERLI
            ) {
                return EVM_POS
            }
            if (chain == Chain.ETHEREUM_CLASSIC__MAINNET ||
                chain == Chain.FANTOM__MAINNET ||
                chain == Chain.RSK__MAINNET ||
                chain == Chain.ETHEREUM__KOVAN ||
                chain == Chain.ETHEREUM__MORDEN ||
                chain == Chain.ETHEREUM__RINKEBY
            ) {
                return EVM_POW
            }
            if (chain == Chain.BITCOIN__MAINNET ||
                chain == Chain.BITCOIN__TESTNET
            ) {
                return BITCOIN
            }
            throw IllegalArgumentException("Unknown type of blockchain: $chain")
        }
    }
}
