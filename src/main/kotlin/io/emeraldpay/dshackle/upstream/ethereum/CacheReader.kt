package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.MethodSpecificReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class CacheReader(
    private val caches: Caches,
) : MethodSpecificReader() {
    companion object {
        private val log = LoggerFactory.getLogger(CacheReader::class.java)
    }

    private val blockByHash = BlockByHash(caches)
    private val blockByHeight = BlockByHeight(caches)
    private val txByHash = TxByHash(caches)
    private val receiptByHash = ReceiptByHash(caches)

    init {
        register(
            "eth_getBlockByHash",
            { params -> params.size >= 2 && params[1] == false },
            blockByHash,
        )
        register(
            "eth_getBlockByNumber",
            { params -> params.size >= 2 && params[0] is String && params[0].toString().startsWith("0x") && params[1] == false },
            blockByHeight,
        )
        register(
            "eth_getTransactionByHash",
            { params -> params.isNotEmpty() },
            txByHash,
        )
        register(
            "eth_getTransactionReceipt",
            { params -> params.isNotEmpty() },
            receiptByHash,
        )
    }

    class BlockByHash(
        private val caches: Caches,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val id =
                try {
                    BlockId.from(key.params[0] as String)
                } catch (t: Throwable) {
                    log.warn("Invalid request ${key.method}(${key.params}")
                    return Mono.empty()
                }
            return caches
                .getBlocksByHash()
                .read(id)
                .filter { it.json != null }
                .map {
                    DshackleResponse(key.id, it.json!!)
                }
        }
    }

    class BlockByHeight(
        caches: Caches,
    ) : DshackleRpcReader {
        private val heights: Reader<Long, BlockId> = caches.getBlockHashByHeight()
        private val blocks: Reader<BlockId, BlockContainer> = caches.getBlocksByHash()

        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val height =
                try {
                    HexQuantity.from(key.params[0] as String).value.longValueExact()
                } catch (t: Throwable) {
                    log.warn("Invalid request ${key.method}(${key.params}")
                    return Mono.empty()
                }
            return heights
                .read(height)
                .flatMap(blocks::read)
                .filter { it.json != null }
                .map {
                    DshackleResponse(key.id, it.json!!)
                }
        }
    }

    class TxByHash(
        private val caches: Caches,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val id =
                try {
                    TxId.from(key.params[0] as String)
                } catch (t: Throwable) {
                    log.warn("Invalid request ${key.method}(${key.params}")
                    return Mono.empty()
                }
            return caches
                .getTxByHash()
                .read(id)
                .filter { it.json != null }
                .map {
                    DshackleResponse(key.id, it.json!!)
                }
        }
    }

    class ReceiptByHash(
        private val caches: Caches,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val id =
                try {
                    TxId.from(key.params[0] as String)
                } catch (t: Throwable) {
                    log.warn("Invalid request ${key.method}(${key.params}")
                    return Mono.empty()
                }
            return caches
                .getReceipts()
                .read(id)
                .map {
                    DshackleResponse(key.id, it)
                }
        }
    }
}
