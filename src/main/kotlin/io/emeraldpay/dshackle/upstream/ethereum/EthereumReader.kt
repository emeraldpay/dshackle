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
import io.emeraldpay.dshackle.data.*
import io.emeraldpay.dshackle.reader.*
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.apache.commons.collections4.Factory
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import java.util.function.Function

/**
 * Reader for the common operations, that wraps caches + native call with quorum verification
 */
open class EthereumReader(
        private val up: Multistream,
        private val caches: Caches,
        private val callMethodsFactory: Factory<CallMethods>
) : Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumReader::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper
    private val balanceCache = CurrentBlockCache<Address, Wei>()
    private val directReader = EthereumDirectReader(up, caches, balanceCache, callMethodsFactory)

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

    private val idToBlockHash = Function<BlockId, BlockHash> { id -> BlockHash.from(id.value) }
    private val blockHashToId = Function<BlockHash, BlockId> { hash -> BlockId.from(hash) }

    private val txHashToId = Function<TransactionId, TxId> { hash -> TxId.from(hash) }
    private val idToTxHash = Function<TxId, TransactionId> { id -> TransactionId.from(id.value) }

    private val blocksByIdAsCont = CompoundReader(
            caches.getBlocksByHash(),
            RekeyingReader(idToBlockHash, directReader.blockReader)
    )

    private val heightByHash = HeightByHashAdding(caches, blocksByIdAsCont)

    fun blocksByHashAsCont(): Reader<BlockHash, BlockContainer> {
        return CompoundReader(
                RekeyingReader(blockHashToId, caches.getBlocksByHash()),
                directReader.blockReader
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
                caches.getBlocksByHeight(),
                directReader.blockByHeightReader
        )
    }

    fun txByHash(): Reader<TransactionId, TransactionJson> {
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
                caches.getTxByHash(),
                RekeyingReader(idToTxHash, directReader.txReader)
        )
    }

    fun balance(): Reader<Address, Wei> {
        //TODO include height as part of cache?
        return CompoundReader(
                balanceCache, directReader.balanceReader
        )
    }

    fun receipts(): Reader<TxId, ByteArray> {
        return caches.getReceipts()
    }

    fun heightByHash(): Reader<BlockId, Long> {
        return heightByHash
    }

    override fun isRunning(): Boolean {
        //TODO should be always running?
        return up.isRunning
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