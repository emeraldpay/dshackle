/*
 * Copyright (c) 2016-2019 ETCDEV GmbH, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle

enum class Chain(val id: Int, val chainCode: String, val chainName: String) {
    UNSPECIFIED(0, "UNSPECIFIED", "Unknown"),
    BITCOIN__MAINNET(1, "BTC", "Bitcoin"), // GRIN(2, "GRIN", "Grin"),

    // Networks with tokens
    ETHEREUM__MAINNET(100, "ETH", "Ethereum"),
    ETHEREUM_CLASSIC__MAINNET(101, "ETC", "Ethereum Classic"),
    FANTOM__MAINNET(
        102,
        "FTM",
        "Fantom"
    ), // LIGHTNING(1001, "BTC_LN", "Bitcoin Lightning"),
    POLYGON_POS__MAINNET(1002, "POLYGON", "Polygon Matic"),
    RSK__MAINNET(1003, "RSK", "Bitcoin RSK"),
    ARBITRUM__MAINNET(
        1004,
        "ARBITRUM",
        "Arbitrum"
    ),
    OPTIMISM__MAINNET(1005, "OPTIMISM", "Optimism"),
    BSC__MAINNET(1006, "BSC", "Binance Smart Chain"),
    POLYGON_ZKEVM__MAINNET(
        1007,
        "POLYGON_ZKEVM",
        "Polygon ZK-EVM"
    ),
    ARBITRUM_NOVA__MAINNET(
        1008,
        "ARBITRUM_NOVA",
        "Arbitrum Nova"
    ),
    ZKSYNC__MAINNET(1009, "ZKSYNC", "ZkSync Era"),
    BASE__MAINNET(1010, "BASE", "Base"),
    LINEA__MAINNET(1011, "LINEA", "Linea"),
    GNOSIS__MAINNET(1012, "GNOSIS", "Gnosis"),
    AVALANCHE__MAINNET(1013, "AVALANCHE", "Avalanche"),
    // STARKNET__MAINNET(1014, "STARKNET", "Starknet"),
    AURORA__MAINNET(1015, "AURORA", "Aurora"),
    // SCROLL__MAINNET(1016, "SCROLL", "Scroll"),
    MANTLE__MAINNET(1017, "MANTLE", "Mantle"),
    KLAYTN__MAINNET(1018, "KLAYTN", "Klaytn"),
    CELO__MAINNET(1019, "CELO_MAINNET", "Celo"),
    MOONBEAM__MAINNET(1020, "MOONBEAM_MAINNET", "Moonbeam"),
    MOONBEAM__MOONRIVER(1021, "MOONBASE_MOONRIVER", "Moonriver"),

    // Testnets
    ETHEREUM__MORDEN(10001, "MORDEN", "Morden Testnet"),
    ETHEREUM__KOVAN(10002, "KOVAN", "Kovan Testnet"),
    BITCOIN__TESTNET(
        10003,
        "TESTNET_BITCOIN",
        "Bitcoin Testnet"
    ), // TESTNET_FLOONET(10004, "FLOONET", "Floonet Testnet"),
    ETHEREUM__GOERLI(10005, "GOERLI", "Goerli Testnet"),
    ETHEREUM__ROPSTEN(
        10006,
        "ROPSTEN",
        "Ropsten Testnet"
    ),
    ETHEREUM__RINKEBY(10007, "RINKEBY", "Rinkeby Testnet"),
    ETHEREUM__SEPOLIA(10008, "SEPOLIA", "Sepolia Testnet"),
    ARBITRUM__GOERLI(
        10009,
        "ARBITRUM_TESTNET",
        "Arbitrum Testnet"
    ),
    OPTIMISM__GOERLI(10010, "OPTIMISM_TESTNET", "Optimism Testnet"),
    POLYGON_ZKEVM__TESTNET(
        10011,
        "POLYGON_ZKEVM_TESTNET",
        "Polygon ZK-EVM Testnet"
    ),
    ZKSYNC__TESTNET(
        10012,
        "ZKS_TESTNET",
        "ZkSync Testnet"
    ),
    POLYGON_POS__MUMBAI(
        10013,
        "POLYGON_POS_MUMBAI",
        "Polygon POS Mumbai Testnet"
    ),
    BASE__GOERLI(10014, "BASE_GOERLI", "Base Goerli Testnet"),
    LINEA__GOERLI(10015, "LINEA_GOERLI", "Linea Goerli Testnet"),
    FANTOM__TESTNET(10016, "FANTOM_TESTNET", "Fantom Testnet"),
    GNOSIS__CHIADO(10017, "GNOSIS_CHIADO", "Gnosis Chiado Testnet"),
    AVALANCHE__FUJI(10018, "AVALANCHE_FUJI", "Avalanche Fuji Testnet"),
    // STARKNET__GOERLI(10019, "STARKNET_GOERLI", "Starknet Goerli"),
    // STARKNET__GOERLI2(10020, "STARKNET_GOERLI2", "Starknet Goerli 2"),
    AURORA__TESTNET(10021, "AURORA_TESTNET", "Aurora Testnet"),
    SCROLL__ALPHANET(10022, "SCROLL_ALPHANET", "Scroll Alphanet"),
    MANTLE__TESTNET(10023, "MANTLE_TESTNET", "Mantle Testnet"),
    KLAYTN__BAOBAB(10024, "KLAYTN_BAOBAB", "Klaytn Baobab"),
    SCROLL__SEPOLIA(10025, "SCROLL_SEPOLIA", "Scroll Sepolia"),
    BSC__TESTNET(10026, "BSC_TESTNET", "Binance Smart Chain Testnet"),
    ETHEREUM__HOLESKY(10027, "ETHEREUM_HOLESKY", "Ethereum Holesky"),
    CELO__ALFAJORES(10028, "CELO_ALFAJORES", "Celo Alfajores"),
    MOONBEAM__ALPHA(10029, "MOONBEAM_ALPHA", "Moonbase Alpha");
    companion object {
        fun byId(id: Int): Chain {
            for (chain in values()) {
                if (chain.id == id) {
                    return chain
                }
            }
            return UNSPECIFIED
        }
    }
}
