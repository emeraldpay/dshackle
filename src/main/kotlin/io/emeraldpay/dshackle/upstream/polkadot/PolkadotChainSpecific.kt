package io.emeraldpay.dshackle.upstream.polkadot

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.foundation.ChainOptions.Options
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.CachingReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.GenericSingleCallValidator
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.LogsOracle
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamRpcMethodsDetector
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DefaultPolkadotMethods
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericEgressSubscription
import io.emeraldpay.dshackle.upstream.generic.GenericIngressSubscription
import io.emeraldpay.dshackle.upstream.generic.LocalReader
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant

object PolkadotChainSpecific : AbstractPollChainSpecific() {
    private val log = LoggerFactory.getLogger(PolkadotChainSpecific::class.java)
    override fun parseBlock(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val response = Global.objectMapper.readValue(data, PolkadotBlockResponse::class.java)
        return api.read(ChainRequest("chain_getBlockHash", ListParams(response.block.header.number)))
            .map { makeBlock(it.getResultAsProcessedString(), response.block.header, data, upstreamId) }
    }

    override fun getFromHeader(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val header = Global.objectMapper.readValue(data, PolkadotHeader::class.java)
        return api.read(ChainRequest("chain_getBlockHash", ListParams(header.number)))
            .map { makeBlock(it.getResultAsProcessedString(), header, data, upstreamId) }
    }

    private fun makeBlock(id: String, header: PolkadotHeader, data: ByteArray, upstreamId: String): BlockContainer {
        return BlockContainer(
            height = header.number.substring(2).toLong(16),
            hash = BlockId.from(id),
            difficulty = BigInteger.ZERO,
            timestamp = Instant.EPOCH,
            full = false,
            json = data,
            parsed = header,
            transactions = emptyList(),
            upstreamId = upstreamId,
            parentHash = BlockId.from(header.parentHash),
        )
    }

    override fun latestBlockRequest(): ChainRequest =
        ChainRequest("chain_getBlock", ListParams())

    override fun listenNewHeadsRequest(): ChainRequest =
        ChainRequest("chain_subscribeNewHeads", ListParams())

    override fun unsubscribeNewHeadsRequest(subId: String): ChainRequest =
        ChainRequest("chain_unsubscribeNewHeads", ListParams(subId))

    override fun localReaderBuilder(
        cachingReader: CachingReader,
        methods: CallMethods,
        head: Head,
        logsOracle: LogsOracle?,
    ): Mono<ChainReader> {
        return Mono.just(LocalReader(methods))
    }

    override fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription {
        return { ms -> GenericEgressSubscription(ms, headScheduler) }
    }

    override fun upstreamValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<UpstreamAvailability>> {
        return listOf(
            GenericSingleCallValidator(
                ChainRequest("system_health", ListParams()),
                upstream,
            ) { data ->
                validate(data, options.minPeers, upstream.getId())
            },
        )
    }

    override fun upstreamSettingsValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<ValidateUpstreamSettingsResult>> {
        return listOf(
            GenericSingleCallValidator(
                ChainRequest("system_chain", ListParams()),
                upstream,
            ) { data ->
                validateSettings(data, chain)
            },
        )
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream): LowerBoundService {
        return PolkadotLowerBoundService(chain, upstream)
    }

    fun validate(data: ByteArray, peers: Int, upstreamId: String): UpstreamAvailability {
        val resp = Global.objectMapper.readValue(data, PolkadotHealth::class.java)
        if (resp.isSyncing) {
            log.warn("Polkadot node {} is in syncing state", upstreamId)
            return UpstreamAvailability.SYNCING
        }
        if (resp.shouldHavePeers && resp.peers < peers) {
            log.warn("Polkadot node {} should but doesn't have enough peers ({} < {})", upstreamId, resp.peers, peers)
            return UpstreamAvailability.IMMATURE
        }

        return UpstreamAvailability.OK
    }

    fun validateSettings(data: ByteArray, chain: Chain): ValidateUpstreamSettingsResult {
        val id = Global.objectMapper.readValue(data, String::class.java)
        return if (chain.chainId.isNotEmpty() && id.lowercase() != chain.chainId.lowercase()) {
            ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
        } else {
            ValidateUpstreamSettingsResult.UPSTREAM_VALID
        }
    }

    override fun makeIngressSubscription(ws: WsSubscriptions): IngressSubscription {
        return GenericIngressSubscription(ws, DefaultPolkadotMethods.subs.map { it.first })
    }

    override fun upstreamRpcMethodsDetector(
        upstream: Upstream,
        config: UpstreamsConfig.Upstream<*>?,
    ): UpstreamRpcMethodsDetector = BasicPolkadotUpstreamRpcMethodsDetector(upstream)
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class PolkadotBlockResponse(
    @JsonProperty("block") var block: PolkadotBlock,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PolkadotBlock(
    @JsonProperty("header") var header: PolkadotHeader,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PolkadotHeader(
    @JsonProperty("parentHash") var parentHash: String,
    @JsonProperty("number") var number: String,
    @JsonProperty("stateRoot") var stateRoot: String,
    @JsonProperty("extrinsicsRoot") var extrinsicsRoot: String,
    @JsonProperty("digest") var digest: PolkadotDigest,
)

data class PolkadotDigest(
    @JsonProperty("logs") var logs: List<String>,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class PolkadotHealth(
    @JsonProperty("peers") var peers: Long,
    @JsonProperty("isSyncing") var isSyncing: Boolean,
    @JsonProperty("shouldHavePeers") var shouldHavePeers: Boolean,
)
