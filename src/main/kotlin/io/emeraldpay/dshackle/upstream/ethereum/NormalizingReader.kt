package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MethodSpecificReader
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.etherjar.hex.HexQuantity
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference

class NormalizingReader(
    private val head: AtomicReference<Head>,
    private val caches: Caches,
    fullBlocksReader: EthereumFullBlocksReader,
) : MethodSpecificReader(),
    DshackleRpcReader {
    companion object {
        private val log = LoggerFactory.getLogger(NormalizingReader::class.java)

        private val HEX_REGEX = Regex("^0x[0-9a-fA-F]+$")
    }

    // TODO make configurable, and it supposed to go into the cache config but read from here
    private val blockInCache = 6

    private val blockByHashFull = BlockByHash(fullBlocksReader)
    private val blockByHashAuto = BlockByHashAuto(blockByHashFull)
    private val blockByNumber = BlockByNumber(fullBlocksReader, caches.getBlockHashByHeight(), blockByHashAuto)
    private val blockByTag = BlockByTag(head, blockByNumber)

    init {
        register(
            "eth_getBlockByHash",
            { params -> params.size >= 2 && acceptBlock(params[0].toString()) && params[1] == true },
            blockByHashFull,
        )
        register(
            "eth_getBlockByNumber",
            { params -> params.size >= 2 && params[0] is String && acceptBlock(params[0].toString()) && params[1] == true },
            blockByNumber,
        )
        register(
            "eth_getBlockByNumber",
            { params ->
                params.size >= 2 &&
                    params[0] is String &&
                    !isBlockOrNumber(params[0].toString()) &&
                    acceptBlock(params[0].toString()) &&
                    params[1] == true
            },
            blockByTag,
        )
    }

    fun isBlockOrNumber(id: String): Boolean = id.matches(HEX_REGEX) && (id.length == 66 || id.length <= 18)

    fun acceptHeight(height: Long): Boolean {
        val current = head.get().getCurrentHeight() ?: return false
        return height >= current - blockInCache
    }

    fun acceptBlock(id: String): Boolean {
        if (id == "latest") {
            return true
        }
        if (!isBlockOrNumber(id)) {
            return false
        }
        val height =
            if (id.length == 66) {
                val blockId =
                    try {
                        BlockId.from(id)
                    } catch (t: Throwable) {
                        return false
                    }
                caches.getHeightByHash(blockId) ?: return false
            } else {
                try {
                    HexQuantity.from(id).value.longValueExact()
                } catch (t: Throwable) {
                    return false
                }
            }
        return acceptHeight(height)
    }

    class BlockByHash(
        private val fullBlocksReader: EthereumFullBlocksReader,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val id =
                try {
                    BlockId.from(key.params[0] as String)
                } catch (t: Throwable) {
                    log.warn("Invalid request ${key.method}(${key.params}")
                    return Mono.empty()
                }
            return fullBlocksReader.byHash
                .read(id)
                .filter { it.json != null }
                .map {
                    DshackleResponse(key.id, it.json!!)
                }
        }
    }

    class BlockByHashAuto(
        private val blockByHash: BlockByHash,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            if (key.params.size != 2) {
                return Mono.empty()
            }
            val full = key.params[1] == true
            return if (full) {
                blockByHash.read(key)
            } else {
                Mono.empty()
            }
        }
    }

    class BlockByNumber(
        private val fullBlocksReader: EthereumFullBlocksReader,
        private val hashByHeight: Reader<Long, BlockId>,
        private val delegate: DshackleRpcReader,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val height =
                try {
                    HexQuantity.from(key.params[0] as String).value.longValueExact()
                } catch (t: Throwable) {
                    log.warn("Invalid request ${key.method}(${key.params}")
                    return Mono.empty()
                }

            val usingCachedHeight =
                hashByHeight
                    .read(height)
                    .map {
                        DshackleRequest(key.id, "eth_getBlockByHash", listOf(it.toHex()) + key.params.drop(1))
                    }.flatMap(delegate::read)

            val readingFromRpc =
                fullBlocksReader.byHeight
                    .read(height)
                    .filter { it.json != null }
                    .map { DshackleResponse(key.id, it.json!!) }

            return usingCachedHeight
                .switchIfEmpty(readingFromRpc)
        }
    }

    class BlockByTag(
        private val head: AtomicReference<Head>,
        private val delegate: DshackleRpcReader,
    ) : DshackleRpcReader {
        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val number: Long
            try {
                val blockRef = key.params[0].toString()
                when {
                    blockRef == "latest" -> {
                        number = head.get().getCurrentHeight() ?: return Mono.empty()
                    }
                    blockRef == "earliest" -> {
                        number = 0
                    }
                    blockRef == "pending" -> {
                        return Mono.empty()
                    }
                    else -> {
                        return Mono.empty()
                    }
                }
            } catch (e: IllegalArgumentException) {
                log.warn("Not a block number", e)
                return Mono.empty()
            }
            val request = DshackleRequest(key.id, "eth_getBlockByNumber", listOf(HexQuantity.from(number).toHex()) + key.params.drop(1))

            return delegate.read(request)
        }
    }
}
