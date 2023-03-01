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
    BITCOIN(1, "BTC", "Bitcoin"), // GRIN(2, "GRIN", "Grin"),

    // Networks with tokens
    ETHEREUM(100, "ETH", "Ethereum"),
    ETHEREUM_CLASSIC(101, "ETC", "Ethereum Classic"),
    FANTOM(
        102,
        "FTM",
        "Fantom"
    ), // LIGHTNING(1001, "BTC_LN", "Bitcoin Lightning"),
    POLYGON(1002, "POLYGON", "Polygon Matic"),
    RSK(1003, "RSK", "Bitcoin RSK"),
    ARBITRUM(
        1004,
        "ARBITRUM",
        "Arbitrum"
    ),
    OPTIMISM(1005, "OPTIMISM", "Optimism"),
    BSC(1006, "BSC", "Binance Smart Chain"), // Testnets
    TESTNET_MORDEN(10001, "MORDEN", "Morden Testnet"),
    TESTNET_KOVAN(10002, "KOVAN", "Kovan Testnet"),
    TESTNET_BITCOIN(
        10003,
        "TESTNET_BITCOIN",
        "Bitcoin Testnet"
    ), // TESTNET_FLOONET(10004, "FLOONET", "Floonet Testnet"),
    TESTNET_GOERLI(10005, "GOERLI", "Goerli Testnet"),
    TESTNET_ROPSTEN(
        10006,
        "ROPSTEN",
        "Ropsten Testnet"
    ),
    TESTNET_RINKEBY(10007, "RINKEBY", "Rinkeby Testnet"),
    TESTNET_SEPOLIA(10008, "SEPOLIA", "Sepolia Testnet"),
    TESTNET_ARBITRUM(
        10009,
        "ARBITRUM_TESTNET",
        "Arbitrum Testnet"
    ),
    TESTNET_OPTIMISM(10010, "OPTIMISM_TESTNET", "Optimism Testnet");

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
