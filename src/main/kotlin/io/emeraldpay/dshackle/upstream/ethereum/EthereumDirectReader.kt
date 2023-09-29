package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.ThrottledLogger
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.reader.RpcReaderFactory
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJsonSnapshot
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.hex.HexQuantity
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.apache.commons.collections4.Factory
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * Common reads from upstream, makes actual calls with applying quorum and retries
 */
class EthereumDirectReader(
    private val up: Multistream,
    private val caches: Caches,
    private val balanceCache: CurrentBlockCache<Address, Wei>,
    private val callMethodsFactory: Factory<CallMethods>,
    private val tracer: Tracer,
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumDirectReader::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper
    var rpcReaderFactory: RpcReaderFactory = RpcReaderFactory.default()

    val blockReader: Reader<BlockHash, Result<BlockContainer>>
    val blockByHeightReader: Reader<Long, Result<BlockContainer>>
    val txReader: Reader<TransactionId, Result<TxContainer>>
    val balanceReader: Reader<Address, Result<Wei>>
    val receiptReader: Reader<TransactionId, Result<ByteArray>>

    init {
        blockReader = object : Reader<BlockHash, Result<BlockContainer>> {
            override fun read(key: BlockHash): Mono<Result<BlockContainer>> {
                val request = JsonRpcRequest("eth_getBlockByHash", listOf(key.toHex(), false))
                return readBlock(request, key.toHex())
            }
        }
        blockByHeightReader = object : Reader<Long, Result<BlockContainer>> {
            override fun read(key: Long): Mono<Result<BlockContainer>> {
                val heightMatcher = Selector.HeightMatcher(key)
                val request = JsonRpcRequest("eth_getBlockByNumber", listOf(HexQuantity.from(key).toHex(), false))
                return readBlock(request, key.toString(), heightMatcher)
            }
        }
        txReader = object : Reader<TransactionId, Result<TxContainer>> {
            override fun read(key: TransactionId): Mono<Result<TxContainer>> {
                val request = JsonRpcRequest("eth_getTransactionByHash", listOf(key.toHex()))
                return readWithQuorum(request) // retries were removed because we use NotNullQuorum which handle errors too
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Tx not read $key")))
                    .flatMap { result ->
                        val tx = objectMapper.readValue(result.data, TransactionJsonSnapshot::class.java)
                        if (tx == null) {
                            Mono.empty()
                        } else {
                            Mono.just(
                                Result(TxContainer.from(tx, result.data), result.upstreamId),
                            )
                        }
                    }
                    .doOnNext { tx ->
                        if (tx.data.blockId != null) {
                            caches.cache(Caches.Tag.REQUESTED, tx.data)
                        }
                    }
            }
        }
        balanceReader = object : Reader<Address, Result<Wei>> {
            override fun read(key: Address): Mono<Result<Wei>> {
                val height = up.getHead().getCurrentHeight()?.let { HexQuantity.from(it).toHex() } ?: "latest"
                val request = JsonRpcRequest("eth_getBalance", listOf(key.toHex(), height))
                return readWithQuorum(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Balance not read $key")))
                    .map {
                        val str = String(it.data)
                        // it's a json string, i.e. wrapped with quotes, ex. _"0x1234"_
                        if (str.startsWith("\"") && str.endsWith("\"")) {
                            Result(
                                Wei.from(str.substring(1, str.length - 1)),
                                it.upstreamId,
                            )
                        } else {
                            throw RpcException(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Not Wei value")
                        }
                    }
                    .retryWhen(Retry.fixedDelay(3, Duration.ofMillis(200)))
                    .doOnNext { value ->
                        balanceCache.put(key, value.data)
                    }
            }
        }
        receiptReader = object : Reader<TransactionId, Result<ByteArray>> {
            override fun read(key: TransactionId): Mono<Result<ByteArray>> {
                val request = JsonRpcRequest("eth_getTransactionReceipt", listOf(key.toHex()))
                return readWithQuorum(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Receipt not read $key")))
                    .flatMap { result ->
                        val receipt = objectMapper.readValue(result.data, TransactionReceiptJson::class.java)
                        if (receipt == null) {
                            log.debug("Empty receipt for txId $key")
                            Mono.empty()
                        } else {
                            caches.cacheReceipt(
                                Caches.Tag.REQUESTED,
                                DefaultContainer(
                                    txId = TxId.from(key),
                                    blockId = BlockId.from(receipt.blockHash),
                                    height = receipt.blockNumber,
                                    json = result.data,
                                    parsed = receipt,
                                ),
                            )
                            Mono.just(
                                result,
                            )
                        }
                    }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun readBlock(
        request: JsonRpcRequest,
        id: String,
        matcher: Selector.Matcher = Selector.empty,
    ): Mono<Result<BlockContainer>> {
        return readWithQuorum(request, matcher)
            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Block not read $id")))
            .retryWhen(Retry.fixedDelay(3, Duration.ofMillis(200)))
            .flatMap { result ->
                val block = objectMapper.readValue(result.data, BlockJson::class.java) as BlockJson<TransactionRefJson>?
                if (block?.checkExtraData() == false) {
                    ThrottledLogger.log(log, "${up.getId()} recieved block with empty extradata from direct reader")
                }
                if (block == null) {
                    Mono.empty()
                } else {
                    Mono.just(
                        Result(
                            BlockContainer.from(block, result.data, "unknown"),
                            result.upstreamId,
                        ),
                    )
                }
            }
            .doOnNext { block ->
                caches.cache(Caches.Tag.REQUESTED, block.data)
            }
    }

    /**
     * Read from an Upstream applying a Quorum specific for that request
     */
    private fun readWithQuorum(
        request: JsonRpcRequest,
        matcher: Selector.Matcher = Selector.empty,
    ): Mono<Result<ByteArray>> {
        return Mono.just(rpcReaderFactory)
            .map {
                val requestMatcher = Selector.Builder()
                    .withMatcher(matcher)
                    .forMethod(request.method)
                    .build()
                it.create(
                    RpcReaderFactory.RpcReaderData(
                        up,
                        request.method,
                        requestMatcher,
                        callMethodsFactory.create().createQuorumFor(request.method),
                        null,
                        tracer,
                    ),
                )
            }.flatMap {
                it.read(request)
            }.map {
                Result(it.value, it.resolvedBy?.getId())
            }
    }

    data class Result<T>(
        val data: T,
        val upstreamId: String?,
    )
}
