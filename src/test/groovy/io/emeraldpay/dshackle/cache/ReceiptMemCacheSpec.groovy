/**
 * Copyright (c) 2021 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.cache

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionReceiptJson
import spock.lang.Specification

import java.time.Instant

class ReceiptMemCacheSpec extends Specification {

    ObjectMapper objectMapper = Global.objectMapper

    def "Add and read"() {
        setup:
        def cache = new ReceiptMemCache()

        def receipt = new TransactionReceiptJson().tap {
            transactionHash = TransactionId.from("0xc7529e79f78f58125abafeaea01fe3abdc6f45c173d5dfb36716cbc526e5b2d1")
            blockHash = BlockHash.from("0x48249c81bfced2e6fe2536126471b73d83c4f21de75f88a16feb57cc566b991b")
            blockNumber = 0xccf6e2
            from = Address.from("0x3a1428354c99b119d891a30d326bad92e36e896a")
            logs = []
        }
        def receiptContainer = new DefaultContainer(
                TxId.from(receipt.transactionHash),
                BlockId.from(receipt.blockHash),
                receipt.blockNumber,
                objectMapper.writeValueAsBytes(receipt),
                receipt
        )

        when:
        cache.add(receiptContainer)
        def act = cache.read(TxId.from(receipt.transactionHash)).block()
        then:
        act != null
        objectMapper.readValue(act, TransactionReceiptJson.class) == receipt
    }

    def "Evict by block"() {
        setup:
        def cache = new ReceiptMemCache()

        def receipt = new TransactionReceiptJson().tap {
            transactionHash = TransactionId.from("0xc7529e79f78f58125abafeaea01fe3abdc6f45c173d5dfb36716cbc526e5b2d1")
            blockHash = BlockHash.from("0x48249c81bfced2e6fe2536126471b73d83c4f21de75f88a16feb57cc566b991b")
            blockNumber = 0xccf6e2
            from = Address.from("0x3a1428354c99b119d891a30d326bad92e36e896a")
            logs = []
        }
        def receiptContainer = new DefaultContainer(
                TxId.from(receipt.transactionHash),
                BlockId.from(receipt.blockHash),
                receipt.blockNumber,
                objectMapper.writeValueAsBytes(receipt),
                receipt
        )

        def blockContainer = new BlockContainer(
                receipt.blockNumber, BlockId.from(receipt.blockHash),
                BigInteger.ONE,
                Instant.now(),
                false,
                "{}".bytes,
                null,
                BlockId.from(receipt.blockHash),
                [TxId.from(receipt.transactionHash)],
                0,
                "unknown"
        )

        when:
        cache.add(receiptContainer)
        cache.evict(blockContainer)
        def act = cache.read(TxId.from(receipt.transactionHash)).block()
        then:
        act == null
    }

}
