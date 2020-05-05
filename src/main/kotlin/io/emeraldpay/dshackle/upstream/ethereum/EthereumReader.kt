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
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.*
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.RpcCall
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
        private val up: Upstream<EthereumApi>,
        private val caches: Caches,
        private val objectMapper: ObjectMapper
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumReader::class.java)
    }

    private var headListener: Disposable? = null
    private val balanceCache = CurrentBlockCache<Address, Wei>()

    private val extractBlock = Function<BlockContainer, BlockJson<TransactionRefJson>> { block ->
        objectMapper
                .readValue(block.json, BlockJson::class.java)
                .withoutTransactionDetails()
    }

    private val extractTx = Function<TxContainer, TransactionJson> { tx ->
        objectMapper
                .readValue(tx.json, TransactionJson::class.java)
    }

    private val blocksDirect: Reader<BlockHash, BlockJson<TransactionRefJson>>
    private val txDirect: Reader<TransactionId, TransactionJson>
    private val balanceDirect: Reader<Address, Wei>

    private val idToBlockHash = Function<BlockId, BlockHash> { id -> BlockHash.from(id.value) }
    private val blockHashToId = Function<BlockHash, BlockId> { hash -> BlockId.from(hash) }

    private val txHashToId = Function<TransactionId, TxId> { hash -> TxId.from(hash) }

    init {
        blocksDirect = object : Reader<BlockHash, BlockJson<TransactionRefJson>> {
            override fun read(key: BlockHash): Mono<BlockJson<TransactionRefJson>> {
                return up.getApi(Selector.empty).flatMap { api ->
                    api.executeAndConvert(Commands.eth().getBlock(key))
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Block not read $key")))
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .doOnNext { block ->
                                caches.cache(Caches.Tag.REQUESTED, BlockContainer.from(block, objectMapper))
                            }
                }
            }
        }
        txDirect = object : Reader<TransactionId, TransactionJson> {
            override fun read(key: TransactionId): Mono<TransactionJson> {
                return up.getApi(Selector.empty).flatMap { api ->
                    api.executeAndConvert(Commands.eth().getTransaction(key))
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Tx not read $key")))
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .doOnNext { tx ->
                                if (tx.blockNumber != null && tx.blockHash != null) {
                                    caches.cache(Caches.Tag.REQUESTED, TxContainer.from(tx, objectMapper))
                                }
                            }
                }
            }
        }
        balanceDirect = object : Reader<Address, Wei> {
            override fun read(key: Address): Mono<Wei> {
                return up.getApi(Selector.empty).flatMap { api ->
                    api.executeAndConvert(Commands.eth().getBalance(key, BlockTag.LATEST))
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Balance not read $key")))
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .doOnNext { value ->
                                balanceCache.put(key, value)
                            }
                }
            }
        }
    }

    fun blocksById(): Reader<BlockId, BlockJson<TransactionRefJson>> {
        return CompoundReader(
                TransformingReader(caches.getBlocksByHash(), extractBlock),
                RekeyingReader(idToBlockHash, blocksDirect)
        )
    }

    fun blocksByHash(): Reader<BlockHash, BlockJson<TransactionRefJson>> {
        return CompoundReader(
                RekeyingReader(
                        blockHashToId,
                        TransformingReader(caches.getBlocksByHash(), extractBlock)
                ),
                blocksDirect
        )
    }

    fun txByHash(): Reader<TransactionId, TransactionJson> {
        return CompoundReader(
                RekeyingReader(
                        txHashToId,
                        TransformingReader(caches.getTxByHash(), extractTx)
                ),
                txDirect
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