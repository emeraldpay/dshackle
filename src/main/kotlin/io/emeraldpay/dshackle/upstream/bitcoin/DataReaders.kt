package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

open class DataReaders(
    private val source: DshackleRpcReader,
    head: AtomicReference<Head>,
) {
    companion object {
        private val log = LoggerFactory.getLogger(DataReaders::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    var extractBlock = ExtractBlock()
    var extractTx = ExtractTx()

    open val mempool = CachingMempoolData(source, head)

    val blockReader: Reader<BlockId, BlockContainer>
    val txReader: Reader<TxId, TxContainer>

    init {
        blockReader =
            object : Reader<BlockId, BlockContainer> {
                override fun read(key: BlockId): Mono<BlockContainer> = getBlock(key, 1)
            }

        txReader =
            object : Reader<TxId, TxContainer> {
                override fun read(key: TxId): Mono<TxContainer> =
                    readBytes(DshackleRequest("getrawtransaction", listOf(key.toHex(), true)))
                        .map(extractTx::extract)
            }
    }

    open fun getBlock(
        hash: BlockId,
        verbosity: Int,
    ): Mono<BlockContainer> {
        if (verbosity == 0) {
            // verbosity=0 returns only a raw transaction, so a BlockContainer cannot be built
            // TODO parse raw transactions?
            return Mono.error(UnsupportedOperationException("getblock with verbosity=0"))
        }
        return readBytes(DshackleRequest("getblock", listOf(hash.toHex(), verbosity)))
            .map(extractBlock::extract)
    }

    open fun getTx(txId: TxId): Mono<TxContainer> = txReader.read(txId)

    open fun getBlockJson(hash: BlockId): Mono<Map<String, Any>> =
        getBlock(hash, 1)
            .map { it.parsed as Map<String, Any> }

    open fun getBlockJson(height: Long): Mono<Map<String, Any>> =
        readJson(DshackleRequest("getblockhash", listOf(height)), String::class.java)
            .map(BlockId.Companion::from)
            .flatMap(this::getBlockJson)

    open fun getTxJson(txid: TxId): Mono<Map<String, Any>> =
        getTx(txid)
            .map { it.getParsed(Map::class.java) as Map<String, Any> }

    fun readBytes(req: DshackleRequest): Mono<ByteArray> =
        source
            .read(req)
            .flatMap(DshackleResponse::requireResult)

    fun <T> readJson(
        req: DshackleRequest,
        clazz: Class<T>,
    ): Mono<T> =
        source
            .read(req)
            .flatMap(DshackleResponse::requireResult)
            .map {
                objectMapper.readValue(it, clazz) as T
            }
}
