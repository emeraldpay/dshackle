package io.emeraldpay.dshackle.upstream.starknet

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.foundation.ChainOptions.Options
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.EmptyEgressSubscription
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.LabelsDetector
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.NoIngressSubscription
import io.emeraldpay.dshackle.upstream.NoopCachingReader
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.generic.CachingReaderBuilder
import io.emeraldpay.dshackle.upstream.generic.ChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericUpstream
import io.emeraldpay.dshackle.upstream.generic.LocalReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import org.springframework.cloud.sleuth.Tracer
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant

object StarknetChainSpecific : ChainSpecific {
    override fun parseBlock(data: ByteArray, upstreamId: String): BlockContainer {
        val block = Global.objectMapper.readValue(data, StarknetBlock::class.java)

        return BlockContainer(
            height = block.number,
            hash = BlockId.from(block.hash),
            difficulty = BigInteger.ZERO,
            timestamp = block.timestamp,
            full = false,
            json = data,
            parsed = block,
            transactions = emptyList(),
            upstreamId = upstreamId,
            parentHash = BlockId.from(block.parent),
        )
    }

    override fun parseHeader(data: ByteArray, upstreamId: String): BlockContainer {
        throw NotImplementedError()
    }

    override fun latestBlockRequest(): JsonRpcRequest =
        JsonRpcRequest("starknet_getBlockWithTxHashes", listOf("latest"))

    override fun listenNewHeadsRequest(): JsonRpcRequest {
        throw NotImplementedError()
    }

    override fun unsubscribeNewHeadsRequest(subId: String): JsonRpcRequest {
        throw NotImplementedError()
    }

    override fun localReaderBuilder(
        cachingReader: CachingReader,
        methods: CallMethods,
        head: Head,
    ): Mono<JsonRpcReader> {
        return Mono.just(LocalReader(methods))
    }

    override fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription {
        return { _ -> EmptyEgressSubscription }
    }

    override fun makeCachingReaderBuilder(tracer: Tracer): CachingReaderBuilder {
        return { _, _, _ -> NoopCachingReader }
    }

    override fun validator(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): UpstreamValidator? {
        return null
    }

    override fun labelDetector(chain: Chain, reader: JsonRpcReader): LabelsDetector? {
        return null
    }

    override fun subscriptionTopics(upstream: GenericUpstream): List<String> {
        return emptyList()
    }

    override fun makeIngressSubscription(ws: WsSubscriptions): IngressSubscription {
        return NoIngressSubscription()
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class StarknetBlock(
    @JsonProperty("block_hash") var hash: String,
    @JsonProperty("block_number") var number: Long,
    @JsonProperty("timestamp") var timestamp: Instant,
    @JsonProperty("parent_hash") var parent: String,
)
