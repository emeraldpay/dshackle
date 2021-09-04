/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.data

import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import org.bouncycastle.util.encoders.Hex
import java.math.BigInteger

class TxId(
        value: ByteArray
) : HashId(value) {

    companion object {
        @JvmStatic
        fun from(id: TransactionId): TxId {
            return TxId(id.bytes)
        }

        @JvmStatic
        fun from(tx: TransactionJson): TxId {
            return from(tx.hash)
        }

        @JvmStatic
        fun from(id: String): TxId {
            val clean = if (id.startsWith("0x")) {
                id.substring(2)
            } else {
                id
            }
            val bytes = Hex.decode(clean)
            return TxId(bytes)
        }
    }
}