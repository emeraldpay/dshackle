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

import io.emeraldpay.dshackle.cache.BlocksMemCache
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.ReceiptRedisCache
import io.emeraldpay.dshackle.cache.TxMemCache
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.test.EthereumUpstreamMock
import io.emeraldpay.dshackle.test.TestingCommons
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.grpc.Chain
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.apache.commons.collections4.Factory
import org.apache.commons.collections4.functors.ConstantFactory
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Instant

class EthereumReaderSpec extends Specification {

    def blockId = BlockId.from("f85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2")
    def blockJson = new BlockJson<TransactionRefJson>().tap { blockJson ->
        blockJson.hash = BlockHash.from(blockId.value)
        blockJson.totalDifficulty = BigInteger.ONE
        blockJson.number = 101
        blockJson.timestamp = Instant.ofEpochSecond(100000000)
        blockJson.transactions = []
        blockJson.uncles = []
    }
    def txId = BlockId.from("a38e7b4d456777c94b46c61a1e4cf52fbdd92acc4444719d1fad77005698c221")
    def txJson = new TransactionJson().tap { json ->
        json.hash = TransactionId.from(txId.value)
        json.blockHash = blockJson.hash
        json.blockNumber = blockJson.number
    }
    Factory<CallMethods> calls = ConstantFactory.constantFactory(new DefaultEthereumMethods(Chain.ETHEREUM))

    def "Block by Id reads from cache"() {
        setup:
        def memCache = Mock(BlocksMemCache) {
            1 * read(blockId) >> Mono.just(BlockContainer.from(blockJson))
        }
        def caches = Caches.newBuilder()
                .setBlockByHash(memCache)
                .build()
        def reader = new EthereumReader(Stub(Multistream), caches, calls)

        when:
        def act = reader.blocksByIdParsed().read(blockId).block()

        then:
        act == blockJson
    }

    def "Block by Id reads from api if cache is empty"() {
        setup:
        def memCache = Mock(BlocksMemCache) {
            1 * read(blockId) >> Mono.empty()
        }
        def caches = Caches.newBuilder()
                .setBlockByHash(memCache)
                .build()
        def api = TestingCommons.api()
        api.answer("eth_getBlockByHash", ["0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2", false], blockJson)

        def upstream = TestingCommons.multistream(api)
        def reader = new EthereumReader(upstream, caches, calls)

        when:
        def act = reader.blocksByIdParsed().read(blockId).block()

        then:
        act == blockJson
    }

    def "Block by Id reads from api if cache failed"() {
        setup:
        def memCache = Mock(BlocksMemCache) {
            1 * read(blockId) >> Mono.error(new IllegalStateException("Test error"))
        }
        def caches = Caches.newBuilder()
                .setBlockByHash(memCache)
                .build()
        def api = TestingCommons.api()
        api.answer("eth_getBlockByHash", ["0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2", false], blockJson)

        def upstream = TestingCommons.multistream(api)
        def reader = new EthereumReader(upstream, caches, calls)

        when:
        def act = reader.blocksByIdParsed().read(blockId).block()

        then:
        act == blockJson
    }

    def "Block by Hash reads from cache"() {
        setup:
        def memCache = Mock(BlocksMemCache) {
            1 * read(blockId) >> Mono.just(BlockContainer.from(blockJson))
        }
        def caches = Caches.newBuilder()
                .setBlockByHash(memCache)
                .build()
        def reader = new EthereumReader(Stub(Multistream), caches, calls)

        when:
        def act = reader.blocksByHashParsed().read(blockJson.hash).block()

        then:
        act == blockJson
    }

    def "Block by Hash reads from api if cache is empty"() {
        setup:
        def memCache = Mock(BlocksMemCache) {
            1 * read(blockId) >> Mono.empty()
        }
        def caches = Caches.newBuilder()
                .setBlockByHash(memCache)
                .build()
        def api = TestingCommons.api()
        api.answer("eth_getBlockByHash", ["0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2", false], blockJson)
        def upstream = TestingCommons.multistream(api)
        def reader = new EthereumReader(upstream, caches, calls)

        when:
        def act = reader.blocksByHashParsed().read(blockJson.hash).block()

        then:
        act == blockJson
    }

    def "Tx by Hash reads from cache"() {
        setup:
        def memCache = Mock(TxMemCache) {
            1 * read(txId) >> Mono.just(TxContainer.from(txJson))
        }
        def caches = Caches.newBuilder()
                .setTxByHash(memCache)
                .build()
        def reader = new EthereumReader(Stub(Multistream), caches, calls)

        when:
        def act = reader.txByHash().read(txJson.hash).block()

        then:
        act == txJson
    }

    def "Tx by Hash reads from api if cache is empty"() {
        setup:
        def memCache = Mock(TxMemCache) {
            1 * read(txId) >> Mono.empty()
        }
        def caches = Caches.newBuilder()
                .setTxByHash(memCache)
                .build()

        def api = TestingCommons.api()
        api.answer("eth_getTransactionByHash", [txJson.hash.toHex()], txJson)
        def upstream = TestingCommons.multistream(api)
        def reader = new EthereumReader(upstream, caches, calls)

        when:
        def act = reader.txByHash().read(txJson.hash).block()

        then:
        act == txJson
    }

    def "Caches balance until block mined"() {
        setup:
        def api = TestingCommons.api()
        // no height
        api.answerOnce("eth_getBalance", ["0x70b91ff87a902b53dc6e2f6bda8bb9b330ccd30c", "latest"], "0x10")
        // height 101 + 1 => 102 => 0x66
        api.answerOnce("eth_getBalance", ["0x70b91ff87a902b53dc6e2f6bda8bb9b330ccd30c", "0x66"], "0xff")
        EthereumUpstreamMock upstream = new EthereumUpstreamMock(Chain.ETHEREUM, api)
        def upstreams = TestingCommons.multistream(upstream)
        def reader = new EthereumReader(upstreams, Caches.default(), calls)
        reader.start()

        when:
        def act = reader.balance().read(Address.from("0x70b91ff87a902b53dc6e2f6bda8bb9b330ccd30c")).block()

        then:
        act == Wei.from("0x10")

        when:
        //now it should use cached value, without actual request
        act = reader.balance().read(Address.from("0x70b91ff87a902b53dc6e2f6bda8bb9b330ccd30c")).block()

        then:
        act == Wei.from("0x10")

        when:
        //move head forward, which should erase cache
        def block2 = blockJson.copy().tap {
            it.number++
            it.totalDifficulty = BigInteger.TWO
        }
        upstream.nextBlock(BlockContainer.from(block2))
        Thread.sleep(50)
        act = reader.balance().read(Address.from("0x70b91ff87a902b53dc6e2f6bda8bb9b330ccd30c")).block()

        then:
        act == Wei.from("0xff")
    }

    def "Read receipt from upstream if cache is empty"() {
        setup:
        def api = TestingCommons.api()
        api.answerOnce("eth_getTransactionReceipt", ["0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2"], [
                transactionHash: "0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2"
        ])
        EthereumUpstreamMock upstream = new EthereumUpstreamMock(Chain.ETHEREUM, api)
        def upstreams = TestingCommons.multistream(upstream)
        def reader = new EthereumReader(upstreams, Caches.default(), calls)
        reader.start()

        when:
        def act = reader.receipts().read(TxId.from("0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2")).block()

        then:
        act != null
        new String(act) == '{"transactionHash":"0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2"}'
    }

    def "Read receipt from cache if available"() {
        setup:
        def api = TestingCommons.api()
        EthereumUpstreamMock upstream = new EthereumUpstreamMock(Chain.ETHEREUM, api)
        def upstreams = TestingCommons.multistream(upstream)
        def receiptCache = Mock(ReceiptRedisCache) {
            1 * it.read(TxId.from("0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2")) >>
                    Mono.just('{"transactionHash":"0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2"}'.bytes)
        }
        def cashes = Caches.newBuilder()
                .setReceipts(receiptCache)
                .build()
        def reader = new EthereumReader(upstreams, cashes, calls)
        reader.start()

        when:
        def act = reader.receipts().read(TxId.from("0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2")).block()

        then:
        act != null
        new String(act) == '{"transactionHash":"0xf85b826fdf98ee0f48f7db001be00472e63ceb056846f4ecac5f0c32878b8ab2"}'
        api.calls.get() == 0
    }
}
