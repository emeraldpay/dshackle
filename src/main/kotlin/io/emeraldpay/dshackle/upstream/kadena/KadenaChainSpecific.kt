package io.emeraldpay.dshackle.upstream.kadena

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.readValue
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
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

object KadenaChainSpecific : AbstractPollChainSpecific() {
    @OptIn(ExperimentalEncodingApi::class)
    override fun parseBlock(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val block = Global.objectMapper.readValue<KadenaHeader>(data)

        val blockHash = Base64.encode(block.id.encodeToByteArray())
        return Mono.just(
            BlockContainer(
                height = block.height,
                hash = BlockId.fromBase64(blockHash),
                difficulty = BigInteger.ZERO,
                timestamp = Instant.EPOCH,
                full = false,
                json = data,
                parsed = block,
                transactions = emptyList(),
                upstreamId = upstreamId,
                parentHash = BlockId.fromBase64(blockHash),
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
        var validators = listOf(
            GenericSingleCallValidator(
                ChainRequest("GET#/cut", RestParams.emptyParams()),
                upstream,
            ) { data ->
                val block = Global.objectMapper.readValue<KadenaHeader>(data)
                if (block.id != "") {
                    UpstreamAvailability.OK
                } else {
                    UpstreamAvailability.UNAVAILABLE
                }
            },
        )

        return validators
    }

    override fun upstreamSettingsValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<ValidateUpstreamSettingsResult>> {
        return emptyList()
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream): LowerBoundService {
        return KadenaLowerBoundService(chain, upstream)
    }

    override fun upstreamSettingsDetector(chain: Chain, upstream: Upstream): UpstreamSettingsDetector {
        return KadenaUpstreamSettingsDetector(upstream)
    }

    override fun latestBlockRequest(): ChainRequest {
        return ChainRequest("GET#/cut", RestParams.emptyParams())
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class KadenaHeader(
    @JsonProperty("height") var height: Long,
    @JsonProperty("weight") var weight: String,
    @JsonProperty("instance") var instance: String,
    @JsonProperty("id") var id: String,
)
