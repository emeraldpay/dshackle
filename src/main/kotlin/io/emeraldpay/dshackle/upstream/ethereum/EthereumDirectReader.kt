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
import io.emeraldpay.dshackle.reader.RequestReaderFactory
import io.emeraldpay.dshackle.upstream.ChainException
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.EthereumCallSelector
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.domain.BlockHash
import io.emeraldpay.dshackle.upstream.ethereum.domain.TransactionId
import io.emeraldpay.dshackle.upstream.ethereum.domain.Wei
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexQuantity
import io.emeraldpay.dshackle.upstream.ethereum.json.BlockJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionJsonSnapshot
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionLogJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionReceiptJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionRefJson
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcException
import io.emeraldpay.dshackle.upstream.ethereum.rpc.RpcResponseError
import io.emeraldpay.dshackle.upstream.finalization.FinalizationType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.apache.commons.collections4.Factory
import org.apache.commons.lang3.exception.ExceptionUtils
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
    var requestReaderFactory: RequestReaderFactory = RequestReaderFactory.default()

    val blockReader: Reader<Request<BlockHash>, Result<BlockContainer>>
    val blockByHeightReader: Reader<Request<Long>, Result<BlockContainer>>
    val txReader: Reader<Request<TransactionId>, Result<TxContainer>>
    val balanceReader: Reader<Address, Result<Wei>>
    val receiptReader: Reader<Request<TransactionId>, Result<ByteArray>>
    val logsByHashReader: Reader<BlockId, Result<List<TransactionLogJson>>>
    val blockByFinalizationReader: Reader<FinalizationType, Result<BlockContainer>>

    init {
        blockReader = object : Reader<Request<BlockHash>, Result<BlockContainer>> {
            override fun read(key: Request<BlockHash>): Mono<Result<BlockContainer>> {
                val request = ChainRequest("eth_getBlockByHash", ListParams(key.requestBy.toHex(), false))
                return readBlock(request, key.requestBy.toHex(), key.upstreamFilter.matcher, key.upstreamFilter.sort)
            }
        }
        blockByHeightReader = object : Reader<Request<Long>, Result<BlockContainer>> {
            override fun read(key: Request<Long>): Mono<Result<BlockContainer>> {
                val request = ChainRequest("eth_getBlockByNumber", ListParams(HexQuantity.from(key.requestBy).toHex(), false))
                return readBlock(request, key.toString(), key.upstreamFilter.matcher, key.upstreamFilter.sort)
            }
        }
        txReader = object : Reader<Request<TransactionId>, Result<TxContainer>> {
            override fun read(key: Request<TransactionId>): Mono<Result<TxContainer>> {
                val request = ChainRequest("eth_getTransactionByHash", ListParams(key.requestBy.toHex()))
                return readWithQuorum(request, key.upstreamFilter.matcher, key.upstreamFilter.sort) // retries were removed because we use NotNullQuorum which handle errors too
                    .timeout(Duration.ofSeconds(5), Mono.error(TimeoutException("Tx not read $key")))
                    .flatMap { result ->
                        val tx = objectMapper.readValue(result.data, TransactionJsonSnapshot::class.java)
                        if (tx == null) {
                            Mono.empty()
                        } else {
                            Mono.just(
                                Result(TxContainer.from(tx, result.data), result.resolvedUpstreamData),
                            )
                        }
                    }
                    .doOnNext { tx ->
                        if (tx.data.blockId != null) {
                            caches.cache(Caches.Tag.REQUESTED, tx.data)
                        }
                    }.onErrorResume {
                        Mono.error(ChainException(request.id, ExceptionUtils.getRootCauseMessage(it)))
                    }
            }
        }

        blockByFinalizationReader = object : Reader<FinalizationType, Result<BlockContainer>> {
            override fun read(key: FinalizationType): Mono<Result<BlockContainer>> {
                val request = ChainRequest("eth_getBlockByNumber", ListParams(key.toBlockRef(), false))
                val tag = when (key) {
                    FinalizationType.FINALIZED_BLOCK -> Selector.Companion.HeightNumberOrTag.Finalized
                    FinalizationType.SAFE_BLOCK -> Selector.Companion.HeightNumberOrTag.Safe
                    else -> null
                }
                return readBlock(
                    request,
                    key.toString(),
                    Selector.empty,
                    tag?.getSort() ?: Selector.Sort.default,
                )
            }
        }

        balanceReader = object : Reader<Address, Result<Wei>> {
            override fun read(key: Address): Mono<Result<Wei>> {
                val height = up.getHead().getCurrentHeight()?.let { HexQuantity.from(it).toHex() } ?: "latest"
                val request = ChainRequest("eth_getBalance", ListParams(key.toHex(), height))
                return readWithQuorum(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Balance not read $key")))
                    .map {
                        val str = String(it.data)
                        // it's a json string, i.e. wrapped with quotes, ex. _"0x1234"_
                        if (str.startsWith("\"") && str.endsWith("\"")) {
                            Result(
                                Wei.from(str.substring(1, str.length - 1)),
                                it.resolvedUpstreamData,
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

        receiptReader = object : Reader<Request<TransactionId>, Result<ByteArray>> {
            override fun read(key: Request<TransactionId>): Mono<Result<ByteArray>> {
                val request = ChainRequest("eth_getTransactionReceipt", ListParams(key.requestBy.toHex()))
                return readWithQuorum(request, key.upstreamFilter.matcher, key.upstreamFilter.sort)
                    .timeout(Duration.ofSeconds(5), Mono.error(TimeoutException("Receipt not read $key")))
                    .flatMap { result ->
                        val receipt = objectMapper.readValue(result.data, TransactionReceiptJson::class.java)
                        if (receipt == null) {
                            log.debug("Empty receipt for txId $key")
                            Mono.empty()
                        } else {
                            caches.cacheReceipt(
                                Caches.Tag.REQUESTED,
                                DefaultContainer(
                                    txId = TxId.from(key.requestBy),
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
                    }.onErrorResume {
                        Mono.error(ChainException(request.id, ExceptionUtils.getRootCauseMessage(it)))
                    }
            }
        }

        logsByHashReader = object : Reader<BlockId, Result<List<TransactionLogJson>>> {
            override fun read(key: BlockId): Mono<Result<List<TransactionLogJson>>> {
                val request = ChainRequest(
                    "eth_getLogs",
                    ListParams(
                        mapOf(
                            "blockHash" to key.toHexWithPrefix(),
                        ),
                    ),
                )
                return EthereumCallSelector(caches).blockByHash(key.toHexWithPrefix())
                    .defaultIfEmpty(Selector.empty).flatMap { matcher ->
                        readWithQuorum(request, matcher)
                            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Logs not read $key")))
                            .flatMap {
                                val logs = objectMapper.readValue(it.data, Array<TransactionLogJson>::class.java)?.toList()
                                if (logs == null) {
                                    log.debug("Empty logs for block $key")
                                    Mono.empty()
                                } else {
                                    Mono.just(Result(logs, it.resolvedUpstreamData))
                                }
                            }
                    }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun readBlock(
        request: ChainRequest,
        id: String,
        matcher: Selector.Matcher = Selector.empty,
        sort: Selector.Sort = Selector.Sort.default,
    ): Mono<Result<BlockContainer>> {
        return readWithQuorum(request, matcher, sort)
            .timeout(Duration.ofSeconds(5), Mono.error(TimeoutException("Block not read $id")))
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
                            result.resolvedUpstreamData,
                        ),
                    )
                }
            }
            .doOnNext { block ->
                caches.cache(Caches.Tag.REQUESTED, block.data)
            }.onErrorResume {
                Mono.error(ChainException(request.id, ExceptionUtils.getRootCauseMessage(it)))
            }
    }

    /**
     * Read from an Upstream applying a Quorum specific for that request
     */
    private fun readWithQuorum(
        request: ChainRequest,
        matcher: Selector.Matcher = Selector.empty,
        sort: Selector.Sort = Selector.Sort.default,
    ): Mono<Result<ByteArray>> {
        return Mono.just(requestReaderFactory)
            .map {
                it.create(
                    RequestReaderFactory.ReaderData(
                        up,
                        Selector.UpstreamFilter(sort, matcher),
                        callMethodsFactory.create().createQuorumFor(request.method),
                        null,
                        tracer,
                    ),
                )
            }.flatMap {
                it.read(request)
            }.map {
                Result(it.value, it.resolvedUpstreamData)
            }
    }

    data class Result<T>(
        val data: T,
        val resolvedUpstreamData: List<Upstream.UpstreamSettingsData>,
    )

    data class Request<T>(
        val requestBy: T,
        val upstreamFilter: Selector.UpstreamFilter,
    )
}
