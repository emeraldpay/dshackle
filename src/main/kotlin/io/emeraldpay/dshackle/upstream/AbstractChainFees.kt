package io.emeraldpay.dshackle.upstream

import com.github.benmanes.caffeine.cache.Caffeine
import io.emeraldpay.api.proto.BlockchainOuterClass
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.EnumMap
import java.util.function.Function

abstract class AbstractChainFees<F, B, TR, T>(
    private val heightLimit: Int,
    private val upstreams: Multistream,
    extractTx: (B) -> List<TR>?
) : ChainFees {

    companion object {
        private val log = LoggerFactory.getLogger(AbstractChainFees::class.java)
    }

    private val txSource = EnumMap<ChainFees.Mode, TxAt<B, TR>>(ChainFees.Mode::class.java)

    init {
        txSource[ChainFees.Mode.AVG_TOP] = TxAtTop(extractTx)
        txSource[ChainFees.Mode.MIN_ALWAYS] = TxAtBottom(extractTx)
        txSource[ChainFees.Mode.AVG_MIDDLE] = TxAtMiddle(extractTx)
        txSource[ChainFees.Mode.AVG_LAST] = TxAtBottom(extractTx)
        txSource[ChainFees.Mode.AVG_T5] = TxAtPos(extractTx, 5)
        txSource[ChainFees.Mode.AVG_T20] = TxAtPos(extractTx, 20)
        txSource[ChainFees.Mode.AVG_T50] = TxAtPos(extractTx, 50)
    }

    override fun estimate(mode: ChainFees.Mode, blocks: Int): Mono<BlockchainOuterClass.EstimateFeeResponse> {
        return usingBlocks(blocks)
            .flatMap { readFeesAt(it, mode) }
            .transform(feeAggregation(mode))
            .next()
            .map(getResponseBuilder())
    }

    // ---

    private val feeCache = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofMinutes(60))
        .build<Pair<Long, ChainFees.Mode>, F>()

    fun usingBlocks(exp: Int): Flux<Long> {
        val useBlocks = exp.coerceAtMost(heightLimit).coerceAtLeast(1)

        val height = upstreams.getHead().getCurrentHeight()
            ?: return Mono.fromCallable { log.warn("Upstream is not ready. No current height") }.thenMany(Mono.empty()) // TODO or throw an exception to build a gRPC error?
        val startBlock: Int = height.toInt() - useBlocks + 1
        if (startBlock < 0) {
            log.warn("Blockchain doesn't have enough blocks. Height: $height")
            return Flux.empty()
        }

        return Flux.range(startBlock, useBlocks).map { it.toLong() }
    }

    fun readFeesAt(height: Long, mode: ChainFees.Mode): Mono<F> {
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

    open fun txSourceFor(mode: ChainFees.Mode): TxAt<B, TR> {
        return txSource[mode] ?: throw IllegalStateException("No TS Source for mode $mode")
    }

    abstract fun readFeesAt(height: Long, selector: TxAt<B, TR>): Mono<F>
    abstract fun feeAggregation(mode: ChainFees.Mode): Function<Flux<F>, Mono<F>>
    abstract fun getResponseBuilder(): Function<F, BlockchainOuterClass.EstimateFeeResponse>

    abstract class TxAt<B, TR>(private val extractTx: Function<B, List<TR>?>) {
        fun get(block: B): TR? {
            val txes = extractTx.apply(block) ?: return null
            return get(txes)
        }

        abstract fun get(transactions: List<TR>): TR?
    }

    class TxAtPos<B, TR>(extractTx: Function<B, List<TR>?>, private val pos: Int) : TxAt<B, TR>(extractTx) {

        override fun get(transactions: List<TR>): TR? {
            val index = pos.coerceAtMost(transactions.size - 1)
            if (index < 0) {
                return null
            }
            return transactions[transactions.size - index - 1]
        }
    }

    class TxAtTop<B, TR>(extractTx: Function<B, List<TR>?>) : TxAt<B, TR>(extractTx) {
        override fun get(transactions: List<TR>): TR? {
            if (transactions.isEmpty()) {
                return null
            }
            return transactions[0]
        }
    }

    class TxAtBottom<B, TR>(extractTx: Function<B, List<TR>?>) : TxAt<B, TR>(extractTx) {
        override fun get(transactions: List<TR>): TR? {
            if (transactions.isEmpty()) {
                return null
            }
            return transactions.last()
        }
    }

    class TxAtMiddle<B, TR>(extractTx: Function<B, List<TR>?>) : TxAt<B, TR>(extractTx) {
        override fun get(transactions: List<TR>): TR? {
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
