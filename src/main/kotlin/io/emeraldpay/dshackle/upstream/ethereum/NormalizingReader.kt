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
    head: AtomicReference<Head>,
    caches: Caches,
    fullBlocksReader: EthereumFullBlocksReader,
) : MethodSpecificReader(), DshackleRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(NormalizingReader::class.java)
    }

    private val blockByHashFull = BlockByHash(fullBlocksReader)
    private val blockByHashAuto = BlockByHashAuto(blockByHashFull)
    private val blockByNumber = BlockByNumber(caches.getBlockHashByHeight(), blockByHashAuto)
    private val blockByTag = BlockByTag(head, blockByNumber)

    init {
        register(
            "eth_getBlockByHash",
            { params -> params.size >= 2 && params[1] == true },
            blockByHashFull
        )
        register(
            "eth_getBlockByNumber",
            { params -> params.size >= 2 && params[0] is String && params[0].toString().startsWith("0x") && params[1] == true },
            blockByNumber
        )
        register(
            "eth_getBlockByNumber",
            { params -> params.size >= 2 && params[0] is String && !params[0].toString().startsWith("0x") && params[1] == true },
            blockByTag
        )
    }

    class BlockByHash(private val fullBlocksReader: EthereumFullBlocksReader) : DshackleRpcReader {

        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val id = try {
                BlockId.from(key.params[0] as String)
            } catch (t: Throwable) {
                log.warn("Invalid request ${key.method}(${key.params}")
                return Mono.empty()
            }
            return fullBlocksReader.read(id)
                .filter { it.json != null }
                .map {
                    DshackleResponse(key.id, it.json!!)
                }
        }
    }

    class BlockByHashAuto(private val blockByHash: BlockByHash) : DshackleRpcReader {

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
        private val hashByHeight: Reader<Long, BlockId>,
        private val delegate: DshackleRpcReader,
    ) : DshackleRpcReader {

        override fun read(key: DshackleRequest): Mono<DshackleResponse> {
            val height = try {
                HexQuantity.from(key.params[0] as String).value.longValueExact()
            } catch (t: Throwable) {
                log.warn("Invalid request ${key.method}(${key.params}")
                return Mono.empty()
            }

            return hashByHeight.read(height)
                .map {
                    DshackleRequest(key.id, "eth_getBlockByHash", listOf(it.toHex()) + key.params.drop(1))
                }
                .flatMap(delegate::read)
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
