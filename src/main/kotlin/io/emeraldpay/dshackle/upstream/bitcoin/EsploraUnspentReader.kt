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

import io.emeraldpay.dshackle.upstream.bitcoin.data.EsploraUnspent
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Function

class EsploraUnspentReader(
    private val esploraClient: EsploraClient,
) : UnspentReader {
    companion object {
        private val log = LoggerFactory.getLogger(EsploraUnspentReader::class.java)
    }

    private val convert: (T: EsploraUnspent) -> SimpleUnspent = { base ->
        SimpleUnspent(
            base.txid,
            base.vout,
            base.value,
        )
    }

    private val convertAll: Function<List<EsploraUnspent>, List<SimpleUnspent>> =
        Function { base ->
            base.map(convert)
        }

    override fun read(key: Address): Mono<List<SimpleUnspent>> =
        esploraClient
            .getUtxo(key)
            .map(convertAll)
}
