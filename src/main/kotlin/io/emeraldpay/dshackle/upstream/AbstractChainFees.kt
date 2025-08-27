package io.emeraldpay.dshackle.upstream

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.api.proto.BlockchainOuterClass
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.EnumMap
import java.util.function.Function

abstract class AbstractChainFees<FEE, BLK, TXID, TRX>(
    private val heightLimit: Int,
    private val upstreams: Multistream,
    extractTx: (BLK) -> List<TXID>?,
) : ChainFees {
    companion object {
        private val log = LoggerFactory.getLogger(AbstractChainFees::class.java)
    }

    private val txSource = EnumMap<ChainFees.Mode, TxAt<BLK, TXID>>(ChainFees.Mode::class.java)

    init {
        txSource[ChainFees.Mode.AVG_TOP] = TxAtTop(extractTx)
        txSource[ChainFees.Mode.MIN_ALWAYS] = TxAtBottom(extractTx)
        txSource[ChainFees.Mode.AVG_MIDDLE] = TxAtMiddle(extractTx)
        txSource[ChainFees.Mode.AVG_LAST] = TxAtBottom(extractTx)
        txSource[ChainFees.Mode.AVG_T5] = TxAtPos(extractTx, 5)
        txSource[ChainFees.Mode.AVG_T20] = TxAtPos(extractTx, 20)
        txSource[ChainFees.Mode.AVG_T50] = TxAtPos(extractTx, 50)
    }

    override fun estimate(
        mode: ChainFees.Mode,
        blocks: Int,
    ): Mono<BlockchainOuterClass.EstimateFeeResponse> =
        usingBlocks(blocks)
            .flatMap { readFeesAt(it, mode) }
            .transform(feeAggregation(mode))
            .next()
            .map(getResponseBuilder())

    // ---

    private val feeCache =
        Caffeine
            .newBuilder()
            .expireAfterWrite(Duration.ofMinutes(60))
            .build<Pair<Long, ChainFees.Mode>, FEE>()

    fun usingBlocks(exp: Int): Flux<Long> {
        val useBlocks = exp.coerceAtMost(heightLimit).coerceAtLeast(1)

        val height =
            upstreams.getHead().getCurrentHeight()
                ?: return Mono
                    .fromCallable {
                        log.warn("Upstream is not ready. No current height")
                    }.thenMany(Mono.empty()) // TODO or throw an exception to build a gRPC error?
        val startBlock: Int = height.toInt() - useBlocks + 1
        if (startBlock < 0) {
            log.warn("Blockchain doesn't have enough blocks. Height: $height")
            return Flux.empty()
        }

        return Flux.range(startBlock, useBlocks).map { it.toLong() }
    }

    fun readFeesAt(
        height: Long,
        mode: ChainFees.Mode,
    ): Mono<FEE> {
        val current = feeCache.getIfPresent(Pair(height, mode))
        if (current != null) {
            return Mono.just(current)
        }
        val txSelector = txSourceFor(mode)
        return readFeesAt(height, txSelector).doOnNext {
            // TODO it may be EMPTY for some blocks (ex. a no tx block), so nothing gets cached and goes to do the same call each time. so do cache empty values to avoid useless requests
            feeCache.put(Pair(height, mode), it!!)
        }
    }

    open fun txSourceFor(mode: ChainFees.Mode): TxAt<BLK, TXID> =
        txSource[mode] ?: throw IllegalStateException("No TS Source for mode $mode")

    abstract fun readFeesAt(
        height: Long,
        selector: TxAt<BLK, TXID>,
    ): Mono<FEE>

    abstract fun feeAggregation(mode: ChainFees.Mode): Function<Flux<FEE>, Mono<FEE>>

    abstract fun getResponseBuilder(): Function<FEE, BlockchainOuterClass.EstimateFeeResponse>

    abstract class TxAt<LOCK, TXID>(
        private val extractTx: Function<LOCK, List<TXID>?>,
    ) {
        fun get(block: LOCK): TXID? {
            val txes = extractTx.apply(block) ?: return null
            return get(txes)
        }

        abstract fun get(transactions: List<TXID>): TXID?
    }

    class TxAtPos<BLK, TXID>(
        extractTx: Function<BLK, List<TXID>?>,
        private val pos: Int,
    ) : TxAt<BLK, TXID>(extractTx) {
        override fun get(transactions: List<TXID>): TXID? {
            val index = pos.coerceAtMost(transactions.size - 1)
            if (index < 0) {
                return null
            }
            return transactions[transactions.size - index - 1]
        }
    }

    class TxAtTop<BLK, TXID>(
        extractTx: Function<BLK, List<TXID>?>,
    ) : TxAt<BLK, TXID>(extractTx) {
        override fun get(transactions: List<TXID>): TXID? {
            if (transactions.isEmpty()) {
                return null
            }
            return transactions[0]
        }
    }

    class TxAtBottom<BLK, TXID>(
        extractTx: Function<BLK, List<TXID>?>,
    ) : TxAt<BLK, TXID>(extractTx) {
        override fun get(transactions: List<TXID>): TXID? {
            if (transactions.isEmpty()) {
                return null
            }
            return transactions.last()
        }
    }

    class TxAtMiddle<BLK, TXID>(
        extractTx: Function<BLK, List<TXID>?>,
    ) : TxAt<BLK, TXID>(extractTx) {
        override fun get(transactions: List<TXID>): TXID? {
            if (transactions.isEmpty()) {
                return null
            }
            if (transactions.size == 1) {
                return transactions[0]
            }
            return transactions[transactions.size / 2]
        }
    }
}
