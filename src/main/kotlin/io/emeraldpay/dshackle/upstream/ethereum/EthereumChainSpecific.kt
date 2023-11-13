package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.foundation.ChainOptions.Options
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.LabelsDetector
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.CallSelector
import io.emeraldpay.dshackle.upstream.calls.EthereumCallSelector
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.AggregatedPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumLabelsDetector
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumWsIngressSubscription
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.NoPendingTxes
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.generic.CachingReaderBuilder
import io.emeraldpay.dshackle.upstream.generic.GenericUpstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

object EthereumChainSpecific : AbstractPollChainSpecific() {
    override fun parseBlock(data: ByteArray, upstreamId: String): BlockContainer {
        return BlockContainer.fromEthereumJson(data, upstreamId)
    }

    override fun parseHeader(data: ByteArray, upstreamId: String): BlockContainer {
        return parseBlock(data, upstreamId)
    }

    override fun latestBlockRequest() = JsonRpcRequest("eth_getBlockByNumber", listOf("latest", false))
    override fun listenNewHeadsRequest(): JsonRpcRequest = JsonRpcRequest("eth_subscribe", listOf("newHeads"))
    override fun unsubscribeNewHeadsRequest(subId: String): JsonRpcRequest =
        JsonRpcRequest("eth_unsubscribe", listOf(subId))

    override fun localReaderBuilder(
        cachingReader: CachingReader,
        methods: CallMethods,
        head: Head,
    ): Mono<JsonRpcReader> {
        return Mono.just(EthereumLocalReader(cachingReader as EthereumCachingReader, methods, head))
    }

    override fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription {
        return { ms ->
            val pendingTxes: PendingTxesSource = (ms.getAll())
                .filter { it is GenericUpstream }
                .map { it as GenericUpstream }
                .filter { it.getIngressSubscription() is EthereumIngressSubscription }
                .mapNotNull {
                    (it.getIngressSubscription() as EthereumIngressSubscription).getPendingTxes()
                }.let {
                    if (it.isEmpty()) {
                        NoPendingTxes()
                    } else if (it.size == 1) {
                        it.first()
                    } else {
                        AggregatedPendingTxes(it)
                    }
                }
            EthereumEgressSubscription(ms, headScheduler, pendingTxes)
        }
    }

    override fun makeCachingReaderBuilder(tracer: Tracer): CachingReaderBuilder {
        return { ms, caches, methodsFactory -> EthereumCachingReader(ms, caches, methodsFactory, tracer) }
    }

    override fun validator(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): UpstreamValidator? {
        return EthereumUpstreamValidator(chain, upstream, options, config)
    }

    override fun labelDetector(chain: Chain, reader: JsonRpcReader): LabelsDetector? {
        return EthereumLabelsDetector(reader, chain)
    }

    override fun makeIngressSubscription(ws: WsSubscriptions): IngressSubscription {
        return EthereumWsIngressSubscription(ws)
    }

    override fun callSelector(caches: Caches): CallSelector {
        return EthereumCallSelector(caches)
    }
}
