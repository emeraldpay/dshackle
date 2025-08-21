package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.DshackleRpcReader
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.reader.RekeyingReader
import io.emeraldpay.dshackle.reader.TransformingReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleRequest
import io.emeraldpay.dshackle.upstream.rpcclient.DshackleResponse
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.hex.HexQuantity
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

/**
 * Common reads from upstream, makes actual calls with applying quorum and retries
 */
open class DataReaders(
    private val source: DshackleRpcReader,
    private val head: AtomicReference<Head>,
) {

    companion object {
        private val log = LoggerFactory.getLogger(DataReaders::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    val blockReader: Reader<BlockHash, BlockContainer>
    val blockReaderById: Reader<BlockId, BlockContainer>
    val blockReaderParsed: Reader<BlockHash, BlockJson<TransactionRefJson>>
    val blockByHeightReader: Reader<Long, BlockContainer>
    val blocksByHeightParsed: Reader<Long, BlockJson<TransactionRefJson>>

    val txReader: Reader<TransactionId, TxContainer>
    val txReaderById: Reader<TxId, TxContainer>
    val txReaderParsed: Reader<TransactionId, TransactionJson>

    val receiptReader: Reader<TransactionId, ByteArray>
    val receiptReaderById: Reader<TxId, ByteArray>

    val balanceReader: Reader<Address, Wei>

    private val extractBlock = { block: BlockContainer ->
        val existing = block.getParsed(BlockJson::class.java)
        if (existing != null) {
            existing.withoutTransactionDetails()
        } else {
            objectMapper
                .readValue(block.json, BlockJson::class.java)
                .withoutTransactionDetails()
        }
    }
    private val extractTx = { tx: TxContainer ->
        tx.getParsed(TransactionJson::class.java) ?: objectMapper.readValue(tx.json, TransactionJson::class.java)
    }

    init {
        blockReader = object : Reader<BlockHash, BlockContainer> {
            override fun read(key: BlockHash): Mono<BlockContainer> {
                val request = DshackleRequest(1, "eth_getBlockByHash", listOf(key.toHex(), false))
                return readBlock(request, key.toHex())
            }
        }
        blockReaderParsed = TransformingReader(
            blockReader,
            extractBlock
        )
        blockReaderById = RekeyingReader(
            BlockId::toEthereumHash, blockReader
        )
        blockByHeightReader = object : Reader<Long, BlockContainer> {
            override fun read(key: Long): Mono<BlockContainer> {
                val request = DshackleRequest(1, "eth_getBlockByNumber", listOf(HexQuantity.from(key).toHex(), false))
                return readBlock(request, key.toString())
            }
        }
        blocksByHeightParsed = TransformingReader(
            blockByHeightReader,
            extractBlock
        )

        txReader = object : Reader<TransactionId, TxContainer> {
            override fun read(key: TransactionId): Mono<TxContainer> {
                val request = DshackleRequest(1, "eth_getTransactionByHash", listOf(key.toHex()))
                return source.read(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(SilentException.Timeout("Tx not read $key")))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .flatMap(DshackleResponse::requireResult)
                    .flatMap { txbytes ->
                        val tx = objectMapper.readValue(txbytes, TransactionJson::class.java)
                        if (tx == null) {
                            Mono.empty()
                        } else {
                            Mono.just(TxContainer.from(tx, txbytes))
                        }
                    }
            }
        }
        txReaderById = RekeyingReader(
            TxId::toEthereumHash, txReader
        )
        txReaderParsed = TransformingReader(
            txReader,
            extractTx
        )

        balanceReader = object : Reader<Address, Wei> {
            override fun read(key: Address): Mono<Wei> {
                val height = head.get().getCurrentHeight()?.let { HexQuantity.from(it).toHex() } ?: "latest"
                val request = DshackleRequest(1, "eth_getBalance", listOf(key.toHex(), height))
                return source.read(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(SilentException.Timeout("Balance not read $key")))
                    .flatMap(DshackleResponse::requireResult)
                    .handle { it, sink ->
                        val str = String(it)
                        // it's a json string, i.e. wrapped with quotes, ex. _"0x1234"_
                        if (str.startsWith("\"") && str.endsWith("\"")) {
                            sink.next(Wei.fromHex(str.substring(1, str.length - 1)))
                        } else {
                            sink.error(RpcException(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Not Wei value"))
                        }
                    }
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
            }
        }
        receiptReader = object : Reader<TransactionId, ByteArray> {
            override fun read(key: TransactionId): Mono<ByteArray> {
                val request = DshackleRequest(1, "eth_getTransactionReceipt", listOf(key.toHex()))
                return source.read(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(SilentException.Timeout("Receipt not read $key")))
                    .flatMap(DshackleResponse::requireResult)
            }
        }
        receiptReaderById = RekeyingReader(
            TxId::toEthereumHash, receiptReader
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun readBlock(request: DshackleRequest, id: String): Mono<BlockContainer> {
        return source.read(request)
            .timeout(Defaults.timeoutInternal, Mono.error(SilentException.Timeout("Block not read $id")))
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
            .flatMap(DshackleResponse::requireResult)
            .flatMap { blockbytes ->
                val block = objectMapper.readValue(blockbytes, BlockJson::class.java) as BlockJson<TransactionRefJson>?
                if (block == null) {
                    Mono.empty<BlockContainer>()
                } else {
                    Mono.just(BlockContainer.from(block, blockbytes))
                }
            }
    }
}
