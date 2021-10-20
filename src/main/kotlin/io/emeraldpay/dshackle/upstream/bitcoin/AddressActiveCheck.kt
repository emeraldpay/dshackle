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
package io.emeraldpay.dshackle.upstream.bitcoin

import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

open class AddressActiveCheck(
    private val esploraClient: EsploraClient
) {

    companion object {
        private val log = LoggerFactory.getLogger(AddressActiveCheck::class.java)
    }

    open fun isActive(address: Address): Mono<Boolean> {
        // TODO cache with bloom filter
        return esploraClient.getTransactions(address).map { it.isNotEmpty() }
    }
}
