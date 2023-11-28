package io.emeraldpay.dshackle.upstream.ethereum

import com.google.common.cache.CacheBuilder
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.etherjar.domain.BlockHash
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import java.time.Duration

class EnrichedMergedHead constructor(
    private val sources: Iterable<Head>,
    private val referenceHead: Head,
    private val headScheduler: Scheduler,
    private val api: Reader<BlockHash, BlockContainer>,
) : Head, Lifecycle {

    private val enrichedBlocks = CacheBuilder.newBuilder()
        .maximumSize(10)
        .build<BlockId, BlockContainer>()
    private val enrichedPromises = CacheBuilder.newBuilder()
        .maximumSize(10)
        .build<BlockId, Sinks.One<BlockContainer>>()
    private var cacheSub: Disposable? = null

    private fun getEnrichBlockMono(id: BlockId): Mono<BlockContainer> {
        val block = enrichedBlocks.getIfPresent(id)
        return if (block != null) {
            Mono.just(block)
        } else {
            enrichedPromises.get(id) {
                Sinks.one()
            }.asMono()
        }
    }

    override fun getFlux(): Flux<BlockContainer> {
        return referenceHead.getFlux().concatMap { block ->
            if (block.enriched) {
                Mono.just(block)
            } else {
                Mono.firstWithValue(
                    getEnrichBlockMono(block.hash),
                    Mono.just(block)
                        .delayElement(Duration.ofSeconds(1))
                        .flatMap {
                            EthereumBlockEnricher.enrich(BlockHash(block.hash.value), api, headScheduler)
                        },
                )
            }
        }
    }

    override fun onBeforeBlock(handler: Runnable) {}

    override fun getCurrentHeight(): Long? {
        return referenceHead.getCurrentHeight()
    }

    override fun getCurrentSlotHeight(): Long? {
        return referenceHead.getCurrentSlotHeight()
    }

    override fun isRunning(): Boolean {
        return cacheSub != null
    }

    override fun start() {
        cacheSub?.dispose()
        sources.forEach { head ->
            if (head is Lifecycle && !head.isRunning()) {
                head.start()
            }
        }
        if (referenceHead is Lifecycle && !referenceHead.isRunning()) {
            referenceHead.start()
        }
        cacheSub = Flux.merge(sources.map { it.getFlux() }).subscribe { block ->
            if (block.enriched) {
                enrichedBlocks.put(block.hash, block)
                enrichedPromises.get(block.hash) { Sinks.one() }.tryEmitValue(block)
            }
        }
    }

    override fun stop() {
        cacheSub?.dispose()
        cacheSub = null
    }

    override fun onSyncingNode(isSyncing: Boolean) {}

    override fun headLiveness(): Flux<Boolean> = Flux.empty()
}
