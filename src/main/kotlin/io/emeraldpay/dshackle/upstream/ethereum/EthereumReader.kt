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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.*
import io.emeraldpay.dshackle.reader.*
import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.hex.HexQuantity
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.RpcResponseError
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.BlockTag
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.function.Function

open class EthereumReader(
        private val up: AggregatedUpstream,
        private val caches: Caches,
        private val objectMapper: ObjectMapper
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumReader::class.java)
    }

    private var headListener: Disposable? = null
    private val balanceCache = CurrentBlockCache<Address, Wei>()

    val extractBlock = Function<BlockContainer, BlockJson<TransactionRefJson>> { block ->
        val existing = block.getParsed(BlockJson::class.java)
        if (existing != null) {
            existing.withoutTransactionDetails()
        } else {
            objectMapper
                    .readValue(block.json, BlockJson::class.java)
                    .withoutTransactionDetails()
        }
    }

    val extractTx = Function<TxContainer, TransactionJson> { tx ->
        tx.getParsed(TransactionJson::class.java) ?: objectMapper.readValue(tx.json, TransactionJson::class.java)
    }

    val asRaw = Function<SourceContainer, ByteArray> { tx ->
        tx.json ?: ByteArray(0)
    }

    val jsonToRaw = Function<Any, ByteArray> { json ->
        objectMapper.writeValueAsBytes(json)
    }

    val blockAsContainer = Function<BlockJson<*>, BlockContainer> { block ->
        BlockContainer.from(block.withoutTransactionDetails(), objectMapper)
    }
    val txAsContainer = Function<TransactionJson, TxContainer> { tx ->
        TxContainer.from(tx, objectMapper)
    }

    private val blocksDirect: Reader<BlockHash, BlockContainer>
    private val blocksByHeightDirect: Reader<Long, BlockContainer>
    private val txDirect: Reader<TransactionId, TxContainer>
    private val balanceDirect: Reader<Address, Wei>

    private val idToBlockHash = Function<BlockId, BlockHash> { id -> BlockHash.from(id.value) }
    private val blockHashToId = Function<BlockHash, BlockId> { hash -> BlockId.from(hash) }

    private val txHashToId = Function<TransactionId, TxId> { hash -> TxId.from(hash) }
    private val idToTxHash = Function<TxId, TransactionId> { id -> TransactionId.from(id.value) }

    private val directResponseBytes = Function<JsonRpcResponse, ByteArray> { resp ->
        if (resp.error != null) {
            throw resp.error.asException()
        } else {
            resp.getResult()
        }
    }

    init {
        blocksDirect = object : Reader<BlockHash, BlockContainer> {
            override fun read(key: BlockHash): Mono<BlockContainer> {
                return up.getDirectApi(Selector.empty).flatMap { api ->
                    val request = JsonRpcRequest("eth_getBlockByHash", listOf(key.toHex(), false))
                    api.read(request)
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Block not read $key")))
                            .map(directResponseBytes)
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .map { blockbytes ->
                                val block = objectMapper.readValue(blockbytes, BlockJson::class.java) as BlockJson<TransactionRefJson>
                                BlockContainer.from(block, blockbytes)
                            }
                            .doOnNext { block ->
                                caches.cache(Caches.Tag.REQUESTED, block)
                            }
                }
            }
        }
        blocksByHeightDirect = object : Reader<Long, BlockContainer> {
            override fun read(key: Long): Mono<BlockContainer> {
                return up.getDirectApi(Selector.empty).flatMap { api ->
                    val request = JsonRpcRequest("eth_getBlockByNumber", listOf(HexQuantity.from(key).toHex(), false))
                    api.read(request)
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Block not read $key")))
                            .map(directResponseBytes)
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .map { blockbytes ->
                                val block = objectMapper.readValue(blockbytes, BlockJson::class.java) as BlockJson<TransactionRefJson>
                                BlockContainer.from(block, blockbytes)
                            }
                            .doOnNext { block ->
                                caches.cache(Caches.Tag.REQUESTED, block)
                            }
                }
            }
        }
        txDirect = object : Reader<TransactionId, TxContainer> {
            override fun read(key: TransactionId): Mono<TxContainer> {
                return up.getDirectApi(Selector.empty).flatMap { api ->
                    val request = JsonRpcRequest("eth_getTransactionByHash", listOf(key.toHex()))
                    api.read(request)
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Tx not read $key")))
                            .map(directResponseBytes)
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .map { txbytes ->
                                val tx = objectMapper.readValue(txbytes, TransactionJson::class.java)
                                TxContainer.from(tx, txbytes)
                            }
                            .doOnNext { tx ->
                                if (tx.blockId != null) {
                                    caches.cache(Caches.Tag.REQUESTED, tx)
                                }
                            }
                }
            }
        }
        balanceDirect = object : Reader<Address, Wei> {
            override fun read(key: Address): Mono<Wei> {
                return up.getDirectApi(Selector.empty).flatMap { api ->
                    val request = JsonRpcRequest("eth_getBalance", listOf(key.toHex(), "latest"))
                    api.read(request)
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Balance not read $key")))
                            .map(directResponseBytes)
                            .map {
                                val str = String(it)
                                if (str.startsWith("\"") && str.endsWith("\"")) {
                                    Wei.from(str.substring(1, str.length - 1))
                                } else {
                                    throw RpcException(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Not Wei value")
                                }
                            }
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .doOnNext { value ->
                                balanceCache.put(key, value)
                            }
                }
            }
        }
    }

    fun blocksByHash(): Reader<BlockHash, BlockJson<TransactionRefJson>> {
        return TransformingReader(
                CompoundReader(
                        RekeyingReader(blockHashToId, caches.getBlocksByHash()),
                        blocksDirect
                ),
                extractBlock
        )
    }

    fun blocksById(): Reader<BlockId, BlockJson<TransactionRefJson>> {
        return TransformingReader(
                CompoundReader(
                        caches.getBlocksByHash(),
                        RekeyingReader(idToBlockHash, blocksDirect)
                ),
                extractBlock
        )
    }

    fun blocksByHashAsCont(): Reader<BlockHash, BlockContainer> {
        return TransformingReader(
                blocksByHash(),
                blockAsContainer
        )
    }

    fun blocksByIdAsCont(): Reader<BlockId, BlockContainer> {
        return TransformingReader(
                blocksById(),
                blockAsContainer
        )
    }

    fun blocksByHeightAsCont(): Reader<Long, BlockContainer> {
        return CompoundReader(
                caches.getBlocksByHeight(),
                blocksByHeightDirect
        )
    }

    fun txByHash(): Reader<TransactionId, TransactionJson> {
        return TransformingReader(
                CompoundReader(
                        RekeyingReader(txHashToId, caches.getTxByHash()),
                        txDirect
                ),
                extractTx
        )
    }

    fun txByHashAsCont(): Reader<TxId, TxContainer> {
        return CompoundReader(
                caches.getTxByHash(),
                RekeyingReader(idToTxHash, txDirect)
        )
    }

    fun balance(): Reader<Address, Wei> {
        return CompoundReader(
                balanceCache, balanceDirect
        )
    }

    override fun isRunning(): Boolean {
        return this.headListener != null
    }

    override fun start() {
        this.headListener = up.getHead().getFlux().subscribe {
            balanceCache.evict()
        }
    }

    override fun stop() {
        val headListener = this.headListener
        this.headListener = null
        headListener?.dispose()
    }
}