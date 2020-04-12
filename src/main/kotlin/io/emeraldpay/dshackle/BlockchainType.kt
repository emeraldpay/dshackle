/**
 * Copyright (c) 2020 ETCDEV GmbH
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

import io.emeraldpay.grpc.Chain

/**
 * Type of the blockchain, architecture-wise
 */
enum class BlockchainType {

    ETHEREUM,
    BITCOIN,
    OTHER;

    companion object {
        @JvmStatic
        fun fromBlockchain(blockchain: Chain): BlockchainType {
            if (blockchain == Chain.ETHEREUM || blockchain == Chain.ETHEREUM_CLASSIC || blockchain == Chain.TESTNET_KOVAN || blockchain == Chain.TESTNET_MORDEN) {
                return ETHEREUM
            }
            if (blockchain == Chain.BITCOIN || blockchain == Chain.TESTNET_BITCOIN) {
                return BITCOIN
            }
            return OTHER
        }
    }
}