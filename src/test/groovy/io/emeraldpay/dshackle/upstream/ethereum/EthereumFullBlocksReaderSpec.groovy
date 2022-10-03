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
package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.TxMemCache
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.apache.commons.codec.binary.Hex
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant

class EthereumFullBlocksReaderSpec extends Specification {

    // sorted
    String hash1 = "0x40d15edaff9acdabd2a1c96fd5f683b3300aad34e7015f34def3c56ba8a7ffb5"
    String hash2 = "0x4aabdaff9acd2f30d15e00ab5dfd5f6c56ba4ea1c968a7ff8d3f34de70153b33"
    String hash3 = "0xa4e7a75dfd5f6a83b3304dc56bfa0abfd3fef01540d15edafc9683f9acd2a13b"
    String hash4 = "0xd3f34def3c56ba4e701540d15edaff9acd2a1c968a7ff83b3300ab5dfd5f6aab"

    ObjectMapper objectMapper = Global.objectMapper

    def tx1 = new TransactionJson().with {
        it.blockNumber = 100
        it.blockHash = BlockHash.from(hash1)
        it.hash = TransactionId.from(hash1)
        it.nonce = 1
        it
    }
    def tx2 = new TransactionJson().with {
        it.blockNumber = 100
        it.blockHash = BlockHash.from(hash1)
        it.hash = TransactionId.from(hash2)
        it.nonce = 2
        it
    }
    def tx3 = new TransactionJson().with {
        it.blockNumber = 101
        it.blockHash = BlockHash.from(hash3)
        it.hash = TransactionId.from(hash3)
        it.nonce = 3
        it
    }

    // no block
    def tx4 = new TransactionJson().with {
        it.blockNumber = 102
        it.blockHash = BlockHash.from(hash4)
        it.hash = TransactionId.from(hash4)
        it.nonce = 4
        it
    }

    // two transactiosn, tx1 and tx2
    def block1 = new BlockJson().with {
        it.number = 100
        it.hash = BlockHash.from(hash1)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = [
                new TransactionRefJson(tx1.hash),
                new TransactionRefJson(tx2.hash)
        ]
        it
    }

    // one transaction, tx3
    def block2 = new BlockJson().with {
        it.number = 101
        it.hash = BlockHash.from(hash3)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = [
                new TransactionRefJson(tx3.hash)
        ]
        it
    }

    // no transactions
    def block3 = new BlockJson().with {
        it.number = 102
        it.hash = BlockHash.from(hash4)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = []
        it
    }

    // all transaction2
    def block4 = new BlockJson().with {
        it.number = 103
        it.hash = BlockHash.from(hash3)
        it.totalDifficulty = BigInteger.ONE
        it.timestamp = Instant.now()
        it.transactions = [
                new TransactionRefJson(tx1.hash),
                new TransactionRefJson(tx2.hash),
                new TransactionRefJson(tx3.hash),
                new TransactionRefJson(tx4.hash)
        ]
        it
    }


    def "Read transactions and return full block"() {
        setup:
        def txes = new TxMemCache()
        def blocks = new BlocksMemCache()

        txes.add(TxContainer.from(tx1))
        txes.add(TxContainer.from(tx2))
        txes.add(TxContainer.from(tx3))
        txes.add(TxContainer.from(tx4))
        blocks.add(BlockContainer.from(block1))
        blocks.add(BlockContainer.from(block2))
        blocks.add(BlockContainer.from(block3))

        def full = new EthereumFullBlocksReader(blocks, txes)

        when:
        def act = full.read(BlockId.from(block1.hash)).block()

        then:
        act != null

        when:
        act = objectMapper.readValue(act.json, BlockJson)

        then:
        act.hash == BlockHash.from(hash1)
        act.number == 100
        act.transactions.size() == 2

        when:
        def transactions = act.transactions.sort { it.hash }
        then:
        transactions[0] instanceof TransactionJson
        transactions[1] instanceof TransactionJson
        //verify it didn't update original block
        block1.transactions[0] instanceof TransactionRefJson
        block1.transactions[1] instanceof TransactionRefJson
        with(transactions[0]) {
            hash == TransactionId.from(hash1)
            nonce == 1
        }
        with(transactions[1]) {
            hash == TransactionId.from(hash2)
            nonce == 2
        }

        // request second block
        when:
        act = full.read(BlockId.from(block2.hash)).block()
        then:
        act != null

        when:
        act = objectMapper.readValue(act.json, BlockJson)

        then:

        act.hash == BlockHash.from(hash3)
        act.number == 101
        act.transactions.size() == 1
        with(act.transactions[0]) {
            hash == TransactionId.from(hash3)
            nonce == 3
        }
    }

    def "Produce block with original order of transactions"() {
        setup:
        def txes = Mock(Reader) {
            1 * it.read(TxId.from(tx1.hash)) >> Mono.just(TxContainer.from(tx1)).delayElement(Duration.ofMillis(50))
            1 * it.read(TxId.from(tx2.hash)) >> Mono.just(TxContainer.from(tx2)).delayElement(Duration.ofMillis(40))
            1 * it.read(TxId.from(tx3.hash)) >> Mono.just(TxContainer.from(tx3)).delayElement(Duration.ofMillis(30))
            1 * it.read(TxId.from(tx4.hash)) >> Mono.just(TxContainer.from(tx4)).delayElement(Duration.ofMillis(20))
        }
        def blocks = new BlocksMemCache()
        blocks.add(BlockContainer.from(block4))

        def full = new EthereumFullBlocksReader(blocks, txes)

        when:
        def act = full.read(BlockId.from(block4.hash)).block()
        act = objectMapper.readValue(act.json, BlockJson)
        def transactions = act.transactions

        then:
        transactions.size() == 4
        transactions[0] instanceof TransactionJson
        transactions[1] instanceof TransactionJson
        transactions[0].hash == TransactionId.from(hash1)
        transactions[1].hash == TransactionId.from(hash2)
        transactions[2].hash == TransactionId.from(hash3)
        transactions[3].hash == TransactionId.from(hash4)
    }

    def "Read block without transactions"() {
        setup:
        def txes = new TxMemCache()
        def blocks = new BlocksMemCache()

        txes.add(TxContainer.from(tx1))
        txes.add(TxContainer.from(tx2))
        txes.add(TxContainer.from(tx3))
        txes.add(TxContainer.from(tx4))
        blocks.add(BlockContainer.from(block1))
        blocks.add(BlockContainer.from(block2))
        blocks.add(BlockContainer.from(block3))

        def full = new EthereumFullBlocksReader(blocks, txes)

        when:
        def act = full.read(BlockId.from(block3.hash)).block()

        then:
        act != null
        act.hash == BlockId.from(hash4)
        act.height == 102
        act.transactions.size() == 0
    }

    def "Return nothing if some of tx are unavailable"() {
        setup:
        def txes = new TxMemCache()
        def blocks = new BlocksMemCache()

        txes.add(TxContainer.from(tx1))
        blocks.add(BlockContainer.from(block1)) //missing tx2 in cache

        def full = new EthereumFullBlocksReader(blocks, txes)

        when:
        def act = full.read(BlockId.from(block1.hash)).block()

        then:
        act == null
    }

    def "Return nothing if no block"() {
        setup:
        def txes = new TxMemCache()
        def blocks = new BlocksMemCache()

        txes.add(TxContainer.from(tx1))
        txes.add(TxContainer.from(tx2))
        txes.add(TxContainer.from(tx3))

        def full = new EthereumFullBlocksReader(blocks, txes)

        when:
        def act = full.read(BlockId.from(block1.hash)).block()

        then:
        act == null
    }

    def "Doesn't change original cached json"() {
        setup:
        def txes = new TxMemCache()
        def blocks = new BlocksMemCache()
        def blockJson = '''
        {
            "number": "0x100001",
            "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
            "timestamp": "0x56cc7b8c",
            "totalDifficulty": "0x6baba0399a0f2e73",
            "extraField": "extraValue",
            "transactions": [
              "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
              "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
            ],
            "extraField2": "extraValue2"
        }
        '''
        blocks.add(BlockContainer.fromEthereumJson(blockJson.bytes, "EthereumFullBlocksReaderSpec"))

        def tx1 = '''
        {
            "hash": "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
            "blockHash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
            "blockNumber": "0x100001",
            "extraField3": "extraValue3"
        }
        '''
        def tx2 = '''
        {
            "hash": "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e",
            "blockHash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
            "blockNumber": "0x100001",
            "from": "0x2a65aca4d5fc5b5c859090a6c34d164135398226",
            "extraField4": "extraValue4"
        }
        '''
        txes.add(TxContainer.from(tx1.bytes))
        txes.add(TxContainer.from(tx2.bytes))

        def full = new EthereumFullBlocksReader(blocks, txes)

        def blockJsonExpected = '''
        {
            "number": "0x100001",
            "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
            "timestamp": "0x56cc7b8c",
            "totalDifficulty": "0x6baba0399a0f2e73",
            "extraField": "extraValue",
            "transactions": [
        ''' + tx1 + ', ' + tx2 +
                '''  ],
            "extraField2": "extraValue2"
        }
        '''
        blockJsonExpected = Global.objectMapper.readValue(blockJsonExpected, Map)

        def prettyJson = Global.objectMapper.writer(new DefaultPrettyPrinter())
        blockJsonExpected = prettyJson.writeValueAsString(blockJsonExpected)

        when:
        def act = full.read(BlockId.from("0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446")).block()
        act = prettyJson.writeValueAsString(Global.objectMapper.readValue(act.json, Map))

        then:
        act.contains("extraValue")
        act.contains("extraValue2")
        act.contains("extraValue3")
        act.contains("extraValue4")
        act == blockJsonExpected
    }

    def "Split block with tx"() {
        setup:
        def blockJson = '''
        {
            "number": "0x100001",
            "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
            "timestamp": "0x56cc7b8c",
            "totalDifficulty": "0x6baba0399a0f2e73",
            "extraField": "extraValue",
            "transactions": [
              "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
              "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
            ],
            "extraField2": "extraValue2"
        }
        '''
        def reader = new EthereumFullBlocksReader(Stub(Reader), Stub(Reader))

        when:
        def act = reader.splitByTransactions(blockJson.bytes)
        then:
        new String(act.getT1()).endsWith('"transactions": [')
        new String(act.getT2()).startsWith('],')
        new String(act.getT2()).trim().endsWith('}')
    }

    def "Split block with tx if formatted with space"() {
        setup:
        def blockJson = '''
        {
            "number": "0x100001",
            "hash": "0x18c68d9ba58772a4409d65d61891b25db03a105a7769ae08ef2cff697921b446",
            "timestamp": "0x56cc7b8c",
            "totalDifficulty": "0x6baba0399a0f2e73",
            "extraField": "extraValue",
            "transactions" : [
              "0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77",
              "0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"
            ] ,
            "extraField2": "extraValue2"
        }
        '''
        def reader = new EthereumFullBlocksReader(Stub(Reader), Stub(Reader))

        when:
        def act = reader.splitByTransactions(blockJson.bytes)
        then:
        new String(act.getT1()).endsWith('"transactions" : [')
        new String(act.getT2()).startsWith('] ,')
        new String(act.getT2()).trim().endsWith('}')
    }

    def "Split block with tx if formatted without space"() {
        setup:
        def blockJson = '{"extraField": "extraValue","transactions":["0x146b8f4b6300c73bb7476359b9f1c5ee3f686a86b2aa673552cf0f9de9a42e77","0xe589a39acea3091b584b650158d08b159aa07e97b8e8cddb8f81cb606e13382e"]}'
        def reader = new EthereumFullBlocksReader(Stub(Reader), Stub(Reader))

        when:
        def act = reader.splitByTransactions(blockJson.bytes)
        then:
        new String(act.getT1()).endsWith('"transactions":[')
        new String(act.getT2()) == ']}'
    }

    def "Extract from buffer without trailing zeroes"() {
        setup:
        def reader = new EthereumFullBlocksReader(Stub(Reader), Stub(Reader))
        when:
        def buffer = ByteBuffer.allocate(8)
        buffer.put(1 as byte)
        buffer.put(2 as byte)
        def act = reader.extractContent(buffer)
        then:
        act.size() == 2
        Hex.encodeHexString(act) == "0102"

        when:
        buffer = ByteBuffer.allocate(8)
        buffer.put(1 as byte)
        buffer.put(2 as byte)
        buffer.put(3 as byte)
        act = reader.extractContent(buffer)
        then:
        act.size() == 3
        Hex.encodeHexString(act) == "010203"

        when:
        buffer = ByteBuffer.allocate(8)
        buffer.put(1 as byte)
        buffer.put(2 as byte)
        buffer.put(3 as byte)
        buffer.put(4 as byte)
        buffer.put(5 as byte)
        buffer.put(6 as byte)
        buffer.put(7 as byte)
        buffer.put(8 as byte)
        act = reader.extractContent(buffer)
        then:
        act.size() == 8
        Hex.encodeHexString(act) == "0102030405060708"
    }
}
