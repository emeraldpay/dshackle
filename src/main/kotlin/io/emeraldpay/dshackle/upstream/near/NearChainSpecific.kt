package io.emeraldpay.dshackle.upstream.near

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.foundation.ChainOptions.Options
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.GenericSingleCallValidator
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.rpcclient.ObjectParams
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant
import java.util.concurrent.TimeUnit

object NearChainSpecific : AbstractPollChainSpecific() {
    override fun parseBlock(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val block = Global.objectMapper.readValue(data, NearBlock::class.java).header

        return Mono.just(
            BlockContainer(
                height = block.height,
                hash = BlockId.fromBase64(block.hash),
                difficulty = BigInteger.ZERO,
                timestamp = Instant.ofEpochMilli(TimeUnit.MILLISECONDS.convert(block.timestamp, TimeUnit.NANOSECONDS)),
                full = false,
                json = data,
                parsed = block,
                transactions = emptyList(),
                upstreamId = upstreamId,
                parentHash = BlockId.fromBase64(block.prevHash),
            ),
        )
    }

    override fun getFromHeader(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        throw NotImplementedError()
    }

    override fun listenNewHeadsRequest(): ChainRequest {
        throw NotImplementedError()
    }

    override fun unsubscribeNewHeadsRequest(subId: String): ChainRequest {
        throw NotImplementedError()
    }

    override fun upstreamValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<UpstreamAvailability>> {
        return listOf(
            GenericSingleCallValidator(
                ChainRequest("status", ListParams()),
                upstream,
            ) { data -> validate(data) },
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
                ChainRequest("status", ListParams()),
                upstream,
            ) { data ->
                validateSettings(data, chain)
            },
        )
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream): LowerBoundService {
        return NearLowerBoundService(chain, upstream)
    }

    fun validate(data: ByteArray): UpstreamAvailability {
        val resp = Global.objectMapper.readValue(data, NearStatus::class.java)
        return if (resp.syncInfo.syncing) {
            UpstreamAvailability.SYNCING
        } else {
            UpstreamAvailability.OK
        }
    }

    fun validateSettings(data: ByteArray, chain: Chain): ValidateUpstreamSettingsResult {
        val resp = Global.objectMapper.readValue(data, NearStatus::class.java)
        return if (chain.chainId.isNotEmpty() && resp.chainId.lowercase() != chain.chainId.lowercase()) {
            ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
        } else {
            ValidateUpstreamSettingsResult.UPSTREAM_VALID
        }
    }

    override fun upstreamSettingsDetector(chain: Chain, upstream: Upstream): UpstreamSettingsDetector {
        return NearUpstreamSettingsDetector(upstream)
    }

    override fun latestBlockRequest(): ChainRequest = // {...}
        ChainRequest("block", ObjectParams("finality" to "optimistic"))
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class NearBlock(
    @JsonProperty("header") var header: NearHeader,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class NearHeader(
    @JsonProperty("height") var height: Long,
    @JsonProperty("hash") var hash: String,
    @JsonProperty("prev_hash") var prevHash: String,
    @JsonProperty("timestamp") var timestamp: Long,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class NearStatus(
    @JsonProperty("chain_id") var chainId: String,
    @JsonProperty("sync_info") var syncInfo: NearSync,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class NearSync(
    @JsonProperty("syncing") var syncing: Boolean,
    @JsonProperty("earliest_block_height") var earliestHeight: Long,
    @JsonProperty("earliest_block_time") var earliestBlockTime: Instant,
)
