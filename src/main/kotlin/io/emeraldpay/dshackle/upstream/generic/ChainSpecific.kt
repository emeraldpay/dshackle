package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.BlockchainType.STARKNET
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.LabelsDetector
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumChainSpecific
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.starknet.StarknetChainSpecific
import org.apache.commons.collections4.Factory
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

typealias SubscriptionBuilder = (Multistream) -> EgressSubscription
typealias LocalReaderBuilder = (CachingReader, CallMethods, Head) -> Mono<JsonRpcReader>
typealias CachingReaderBuilder = (Multistream, Caches, Factory<CallMethods>) -> CachingReader

interface ChainSpecific {
    fun parseBlock(data: JsonRpcResponse, upstreamId: String): BlockContainer

    fun latestBlockRequest(): JsonRpcRequest

    fun localReaderBuilder(cachingReader: CachingReader, methods: CallMethods, head: Head): Mono<JsonRpcReader>

    fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription

    fun makeCachingReaderBuilder(tracer: Tracer): CachingReaderBuilder

    fun validator(chain: Chain, upstream: Upstream, options: ChainOptions.Options, config: ChainConfig): UpstreamValidator?

    fun labelDetector(chain: Chain, reader: JsonRpcReader): LabelsDetector?

    fun subscriptionTopics(upstream: GenericUpstream): List<String>
}

object ChainSpecificRegistry {

    @JvmStatic
    fun resolve(chain: Chain): ChainSpecific {
        if (BlockchainType.from(chain) == STARKNET) {
            return StarknetChainSpecific
        }
        return EthereumChainSpecific
    }
}
