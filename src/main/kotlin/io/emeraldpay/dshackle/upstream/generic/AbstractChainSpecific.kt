package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.EmptyEgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.LogsOracle
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.NoIngressSubscription
import io.emeraldpay.dshackle.upstream.NoopCachingReader
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamRpcModulesDetector
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.CallSelector
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.finalization.FinalizationDetector
import io.emeraldpay.dshackle.upstream.finalization.NoopFinalizationDetector
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

abstract class AbstractChainSpecific : ChainSpecific {
    override fun localReaderBuilder(
        cachingReader: CachingReader,
        methods: CallMethods,
        head: Head,
        logsOracle: LogsOracle?,
    ): Mono<ChainReader> {
        return Mono.just(LocalReader(methods))
    }

    override fun finalizationDetectorBuilder(): FinalizationDetector {
        return NoopFinalizationDetector()
    }

    override fun makeCachingReaderBuilder(tracer: Tracer): CachingReaderBuilder {
        return { _, _, _ -> NoopCachingReader }
    }

    override fun upstreamSettingsDetector(
        chain: Chain,
        upstream: Upstream,
    ): UpstreamSettingsDetector? {
        return null
    }

    override fun upstreamRpcModulesDetector(upstream: Upstream): UpstreamRpcModulesDetector? {
        return null
    }

    override fun makeIngressSubscription(ws: WsSubscriptions): IngressSubscription {
        return NoIngressSubscription()
    }

    override fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription {
        return { _ -> EmptyEgressSubscription }
    }

    override fun callSelector(caches: Caches): CallSelector? {
        return null
    }
}

abstract class AbstractPollChainSpecific : AbstractChainSpecific() {

    override fun getLatestBlock(api: ChainReader, upstreamId: String): Mono<BlockContainer> {
        return api.read(latestBlockRequest()).map {
            parseBlock(it.getResult(), upstreamId)
        }
    }

    abstract fun latestBlockRequest(): ChainRequest

    abstract fun parseBlock(data: ByteArray, upstreamId: String): BlockContainer
}
