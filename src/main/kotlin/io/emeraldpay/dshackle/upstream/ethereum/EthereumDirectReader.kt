package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CurrentBlockCache
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.quorum.QuorumReaderFactory
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.domain.Wei
import io.emeraldpay.etherjar.hex.HexQuantity
import io.emeraldpay.etherjar.rpc.RpcException
import io.emeraldpay.etherjar.rpc.RpcResponseError
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
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
    private val tracer: Tracer
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumDirectReader::class.java)
    }

    private val objectMapper: ObjectMapper = Global.objectMapper
    var quorumReaderFactory: QuorumReaderFactory = QuorumReaderFactory.default()

    val blockReader: Reader<BlockHash, BlockContainer>
    val blockByHeightReader: Reader<Long, BlockContainer>
    val txReader: Reader<TransactionId, TxContainer>
    val balanceReader: Reader<Address, Wei>
    val receiptReader: Reader<TransactionId, ByteArray>

    init {
        blockReader = object : Reader<BlockHash, BlockContainer> {
            override fun read(key: BlockHash): Mono<BlockContainer> {
                val request = JsonRpcRequest("eth_getBlockByHash", listOf(key.toHex(), false))
                return readBlock(request, key.toHex())
            }
        }
        blockByHeightReader = object : Reader<Long, BlockContainer> {
            override fun read(key: Long): Mono<BlockContainer> {
                val heightMatcher = Selector.HeightMatcher(key)
                val request = JsonRpcRequest("eth_getBlockByNumber", listOf(HexQuantity.from(key).toHex(), false))
                return readBlock(request, key.toString(), heightMatcher)
            }
        }
        txReader = object : Reader<TransactionId, TxContainer> {
            override fun read(key: TransactionId): Mono<TxContainer> {
                val request = JsonRpcRequest("eth_getTransactionByHash", listOf(key.toHex()))
                return readWithQuorum(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Tx not read $key")))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .flatMap { txbytes ->
                        val tx = objectMapper.readValue(txbytes, TransactionJson::class.java)
                        if (tx == null) {
                            Mono.empty()
                        } else {
                            Mono.just(TxContainer.from(tx, txbytes))
                        }
                    }
                    .doOnNext { tx ->
                        if (tx.blockId != null) {
                            caches.cache(Caches.Tag.REQUESTED, tx)
                        }
                    }
            }
        }
        balanceReader = object : Reader<Address, Wei> {
            override fun read(key: Address): Mono<Wei> {
                val height = up.getHead().getCurrentHeight()?.let { HexQuantity.from(it).toHex() } ?: "latest"
                val request = JsonRpcRequest("eth_getBalance", listOf(key.toHex(), height))
                return readWithQuorum(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Balance not read $key")))
                    .map {
                        val str = String(it)
                        // it's a json string, i.e. wrapped with quotes, ex. _"0x1234"_
                        if (str.startsWith("\"") && str.endsWith("\"")) {
                            Wei.from(str.substring(1, str.length - 1))
                        } else {
                            throw RpcException(RpcResponseError.CODE_UPSTREAM_INVALID_RESPONSE, "Not Wei value")
                        }
                    }
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .doOnNext { value ->
                        balanceCache.put(key, value)
                    }
            }
        }
        receiptReader = object : Reader<TransactionId, ByteArray> {
            override fun read(key: TransactionId): Mono<ByteArray> {
                val request = JsonRpcRequest("eth_getTransactionReceipt", listOf(key.toHex()))
                return readWithQuorum(request)
                    .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Receipt not read $key")))
                    .flatMap { json ->
                        val receipt = objectMapper.readValue(json, TransactionReceiptJson::class.java)
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
                                    json = json,
                                    parsed = receipt
                                )
                            )
                            Mono.just(json)
                        }
                    }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun readBlock(
        request: JsonRpcRequest,
        id: String,
        matcher: Selector.Matcher = Selector.empty
    ): Mono<BlockContainer> {
        return readWithQuorum(request, matcher)
            .timeout(Defaults.timeoutInternal, Mono.error(TimeoutException("Block not read $id")))
            .retryWhen(Retry.fixedDelay(3, Duration.ofMillis(200)))
            .flatMap { blockbytes ->
                val block = objectMapper.readValue(blockbytes, BlockJson::class.java) as BlockJson<TransactionRefJson>?
                if (block == null) {
                    Mono.empty<BlockContainer>()
                } else {
                    Mono.just(BlockContainer.from(block, blockbytes, "unknown"))
                }
            }
            .doOnNext { block ->
                caches.cache(Caches.Tag.REQUESTED, block)
            }
    }

    /**
     * Read from an Upstream applying a Quorum specific for that request
     */
    private fun readWithQuorum(request: JsonRpcRequest, matcher: Selector.Matcher = Selector.empty): Mono<ByteArray> {
        return Mono.just(quorumReaderFactory)
            .map {
                it.create(
                    up.getApiSource(matcher),
                    callMethodsFactory.create().createQuorumFor(request.method),
                    // we do not use Signer for internal requests because it doesn't make much sense
                    null,
                    tracer
                )
            }.flatMap {
                it.read(request)
            }.map {
                it.value
            }
    }
}
