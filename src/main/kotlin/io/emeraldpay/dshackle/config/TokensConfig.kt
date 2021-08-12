/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.config

import io.emeraldpay.grpc.BlockchainType
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address

class TokensConfig(
        val tokens: List<Token>
) {

    class Token {
        // reference id, used by get balance and others
        var id: String? = null
        var blockchain: Chain? = null

        // coin name
        var name: String? = null
        var type: Type? = null;
        var address: String? = null

        fun validate(): String? {
            return when {
                id.isNullOrBlank() -> "id"
                blockchain == null -> "blockchain"
                name.isNullOrBlank() -> "name"
                type == null -> type
                address.isNullOrBlank() -> "address"
                blockchain != null
                        && BlockchainType.from(blockchain!!) == BlockchainType.ETHEREUM
                        && !Address.isValidAddress(address) -> "address"
                else -> null
            }
        }
    }

    enum class Type {
        ERC20
    }

}