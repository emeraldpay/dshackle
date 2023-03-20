package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.CompoundReader
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.reader.TransformingReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.util.function.Tuples

class BitcoinCacheReader(
    private val caches: Caches,
) : DshackleRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinCacheReader::class.java)
    }

    private val blockRaw = object : Reader<BlockId, ByteArray> {
        override fun read(key: BlockId): Mono<ByteArray> {
            return (caches.getGenericCache("block-raw")?.read(key.toHex()) ?: Mono.empty())
                .map(Hex::encodeHexString)
                .map(String::toByteArray)
        }
    }

    private val blockJson = TransformingReader(caches.getBlocksByHash()) { it.json!! }

    private val txJson = TransformingReader(caches.getTxByHash()) { it.json!! }

    private val txRawFromJson = TransformingReader(caches.getTxByHash()) {
        val parsed = it.getParsed(Map::class.java) ?: Global.objectMapper.readValue(it.json!!, Map::class.java)
        if (parsed.containsKey("hex")) {
            (parsed["hex"] as String).toByteArray()
        } else {
            null
        }
    }

    private val txRaw = object : Reader<TxId, ByteArray> {
        override fun read(key: TxId): Mono<ByteArray> {
            return (caches.getGenericCache("tx-raw")?.read(key.toHex()) ?: Mono.empty())
                .map(Hex::encodeHexString)
                .map(String::toByteArray)
        }
    }

    fun readBlock(blockId: BlockId, verbosity: Int): Mono<ByteArray> {
        return when (verbosity) {
            0 -> blockRaw.read(blockId)
            1 -> blockJson.read(blockId)
            // TODO process verbosity 2 by splitting into transactions and processing them individually
            else -> Mono.empty()
        }
    }

    fun readRawTx(txId: TxId, verbose: Boolean): Mono<ByteArray> {
        return if (verbose) {
            txJson.read(txId)
        } else {
            CompoundReader(
                txRaw,
                txRawFromJson
            ).read(txId)
        }
    }

    fun processGetTxRequest(key: DshackleRequest): Mono<ByteArray> {
        return Mono.fromCallable {
            // #1 - txid - string, required
            // #2 - verbose - boolean, optional, default=false
            // #3 - blockhash - string, optional
            if (key.params.size !in 1..3) {
                return@fromCallable null
            }
            val txid = TxId.from(key.params[0].toString())
            val verbose = key.params[1] as Boolean
            Tuples.of(txid, verbose)
        }
            .onErrorResume { t ->
                log.warn("Invalid request ${key.method}(${key.params})")
                Mono.empty()
            }
            .flatMap {
                readRawTx(it!!.t1, it.t2)
            }
    }

    fun processGetBlockRequest(key: DshackleRequest): Mono<ByteArray> {
        return Mono.fromCallable {
            if (key.params.size !in 1..2) {
                return@fromCallable null
            }
            val hash = BlockId.from(key.params[0].toString())
            val verbosity = if (key.params.size >= 2) {
                key.params[1].let { if (it is Number) it.toInt() else it.toString().toInt() }
            } else {
                1
            }
            Tuples.of(hash, verbosity)
        }
            .onErrorResume { t ->
                log.warn("Invalid request ${key.method}(${key.params})")
                Mono.empty()
            }
            .flatMap {
                readBlock(it!!.t1, it.t2)
            }
    }

    override fun read(key: DshackleRequest): Mono<DshackleResponse> {
        if (key.method == "getblock") {
            return processGetBlockRequest(key)
                .map { DshackleResponse(key.id, it) }
        }
        if (key.method == "getrawtransaction") {
            return processGetTxRequest(key)
                .map { DshackleResponse(key.id, it) }
        }
        return Mono.empty()
    }
}
