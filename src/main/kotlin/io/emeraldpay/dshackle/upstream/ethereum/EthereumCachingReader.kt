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
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.cache.HeightByHashAdding
import io.emeraldpay.dshackle.commons.CACHE_BLOCK_BY_HASH_READER
import io.emeraldpay.dshackle.commons.CACHE_BLOCK_BY_HEIGHT_READER
import io.emeraldpay.dshackle.commons.CACHE_HEIGHT_BY_HASH_READER
import io.emeraldpay.dshackle.commons.CACHE_RECEIPTS_READER
import io.emeraldpay.dshackle.commons.CACHE_TX_BY_HASH_READER
import io.emeraldpay.dshackle.commons.DIRECT_QUORUM_RPC_READER
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.SourceContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.reader.RekeyingReader
import io.emeraldpay.dshackle.reader.SpannedReader
import io.emeraldpay.dshackle.reader.TransformingReader
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJsonSnapshot
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.apache.commons.collections4.Factory
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.Tracer
import java.util.function.Function

/**
 * Reader for the common operations, that use the cache when data is available or a native call with quorum verification
 */
open class EthereumCachingReader(
    private val up: Multistream,
    private val caches: Caches,
    private val callMethodsFactory: Factory<CallMethods>,
    private val tracer: Tracer
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumCachingReader::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper
    private val balanceCache = CurrentBlockCache<Address, Wei>()
    private val directReader = EthereumDirectReader(up, caches, balanceCache, callMethodsFactory, tracer)

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

    val extractTx = Function<TxContainer, TransactionJsonSnapshot> { tx ->
        tx.getParsed(TransactionJsonSnapshot::class.java) ?: objectMapper.readValue(tx.json, TransactionJsonSnapshot::class.java)
    }

    val asRaw = Function<SourceContainer, ByteArray> { tx ->
        tx.json ?: ByteArray(0)
    }

    private val idToBlockHash = Function<BlockId, BlockHash> { id -> BlockHash.from(id.value) }
    private val blockHashToId = Function<BlockHash, BlockId> { hash -> BlockId.from(hash) }

    private val txHashToId = Function<TransactionId, TxId> { hash -> TxId.from(hash) }
    private val idToTxHash = Function<TxId, TransactionId> { id -> TransactionId.from(id.value) }

    private val blocksByIdAsCont = CompoundReader(
        SpannedReader(caches.getBlocksByHash(), tracer, CACHE_BLOCK_BY_HASH_READER),
        SpannedReader(RekeyingReader(idToBlockHash, directReader.blockReader), tracer, DIRECT_QUORUM_RPC_READER)
    )

    private val heightByHash =
        SpannedReader(HeightByHashAdding(caches, blocksByIdAsCont), tracer, CACHE_HEIGHT_BY_HASH_READER)

    fun blocksByHashAsCont(): Reader<BlockHash, BlockContainer> {
        return CompoundReader(
            SpannedReader(RekeyingReader(blockHashToId, caches.getBlocksByHash()), tracer, CACHE_BLOCK_BY_HASH_READER),
            SpannedReader(directReader.blockReader, tracer, DIRECT_QUORUM_RPC_READER)
        )
    }

    fun blocksByHashParsed(): Reader<BlockHash, BlockJson<TransactionRefJson>> {
        return TransformingReader(
            blocksByHashAsCont(),
            extractBlock
        )
    }

    fun blocksByIdParsed(): Reader<BlockId, BlockJson<TransactionRefJson>> {
        return TransformingReader(
            blocksByIdAsCont(),
            extractBlock
        )
    }

    open fun blocksByIdAsCont(): Reader<BlockId, BlockContainer> {
        return blocksByIdAsCont
    }

    open fun blocksByHeightAsCont(): Reader<Long, BlockContainer> {
        return CompoundReader(
            SpannedReader(caches.getBlocksByHeight(), tracer, CACHE_BLOCK_BY_HEIGHT_READER),
            SpannedReader(directReader.blockByHeightReader, tracer, DIRECT_QUORUM_RPC_READER)
        )
    }

    open fun blocksByHeightParsed(): Reader<Long, BlockJson<TransactionRefJson>> {
        return TransformingReader(
            blocksByHeightAsCont(),
            extractBlock
        )
    }

    open fun txByHash(): Reader<TransactionId, TransactionJsonSnapshot> {
        return TransformingReader(
            CompoundReader(
                RekeyingReader(txHashToId, caches.getTxByHash()),
                directReader.txReader
            ),
            extractTx
        )
    }

    open fun txByHashAsCont(): Reader<TxId, TxContainer> {
        return CompoundReader(
            SpannedReader(caches.getTxByHash(), tracer, CACHE_TX_BY_HASH_READER),
            SpannedReader(RekeyingReader(idToTxHash, directReader.txReader), tracer, DIRECT_QUORUM_RPC_READER)
        )
    }

    fun balance(): Reader<Address, Wei> {
        // TODO include height as part of cache?
        return CompoundReader(
            balanceCache, directReader.balanceReader
        )
    }

    fun receipts(): Reader<TxId, ByteArray> {
        val requested = RekeyingReader(
            { txid: TxId -> TransactionId.from(txid.value) },
            directReader.receiptReader
        )
        return CompoundReader(
            SpannedReader(caches.getReceipts(), tracer, CACHE_RECEIPTS_READER),
            SpannedReader(requested, tracer, DIRECT_QUORUM_RPC_READER)
        )
    }

    fun heightByHash(): Reader<BlockId, Long> {
        return heightByHash
    }

    override fun isRunning(): Boolean {
        // TODO should be always running?
        return true // up.isRunning
    }

    override fun start() {
        val evictCaches: Runnable = Runnable {
            balanceCache.evict()
        }
        up.getHead().onBeforeBlock(evictCaches)
    }

    override fun stop() {
    }
}
