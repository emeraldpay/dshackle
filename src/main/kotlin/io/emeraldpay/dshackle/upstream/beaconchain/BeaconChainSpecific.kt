package io.emeraldpay.dshackle.upstream.beaconchain

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
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

object BeaconChainSpecific : AbstractPollChainSpecific() {
    override fun getFromHeader(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        throw NotImplementedError()
    }

    override fun listenNewHeadsRequest(): ChainRequest {
        throw NotImplementedError()
    }

    override fun unsubscribeNewHeadsRequest(subId: String): ChainRequest {
        throw NotImplementedError()
    }

    override fun latestBlockRequest(): ChainRequest {
        return ChainRequest("GET#/eth/v1/beacon/headers/head", RestParams.emptyParams())
    }

    override fun parseBlock(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val blockHeader = Global.objectMapper.readValue<BeaconChainBlockHeader>(data)

        return Mono.just(
            BlockContainer(
                height = blockHeader.height,
                hash = BlockId.from(blockHeader.hash),
                difficulty = BigInteger.ZERO,
                timestamp = Instant.EPOCH,
                full = false,
                json = data,
                parsed = blockHeader,
                transactions = emptyList(),
                upstreamId = upstreamId,
                parentHash = BlockId.from(blockHeader.parentHash),
            ),
        )
    }

    override fun upstreamSettingsDetector(chain: Chain, upstream: Upstream): UpstreamSettingsDetector {
        return BeaconChainUpstreamSettingsDetector(upstream)
    }

    override fun upstreamValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<UpstreamAvailability>> {
        var validators = listOf(
            GenericSingleCallValidator(
                ChainRequest("GET#/eth/v1/node/health", RestParams.emptyParams()),
                upstream,
            ) { _ -> UpstreamAvailability.OK },
        )
        if (options.validateSyncing) {
            validators += GenericSingleCallValidator(
                ChainRequest("GET#/eth/v1/node/syncing", RestParams.emptyParams()),
                upstream,
            ) { data ->
                val syncing = Global.objectMapper.readValue(data, BeaconChainSyncing::class.java).data.isSyncing
                upstream.getHead().onSyncingNode(syncing)
                if (syncing) {
                    UpstreamAvailability.SYNCING
                } else {
                    UpstreamAvailability.OK
                }
            }
        }
        if (options.validatePeers && options.minPeers > 0) {
            validators += GenericSingleCallValidator(
                ChainRequest("GET#/eth/v1/node/peer_count", RestParams.emptyParams()),
                upstream,
            ) { data ->
                val connected = Global.objectMapper.readValue(
                    data,
                    BeaconChainPeers::class.java,
                ).data.connected.toInt()
                if (connected < options.minPeers) UpstreamAvailability.IMMATURE else UpstreamAvailability.OK
            }
        }
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
        return BeaconChainLowerBoundService(chain, upstream)
    }
}

data class BeaconChainBlockHeader(
    val hash: String,
    val parentHash: String,
    val height: Long,
)

class BeaconChainBlockHeaderDeserializer : JsonDeserializer<BeaconChainBlockHeader>() {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): BeaconChainBlockHeader {
        val node = p.readValueAsTree<JsonNode>()
        val data = node["data"]

        val hash = data["root"].textValue()
        val headerData = data["header"]["message"]
        val height = headerData["slot"].textValue().toLong()
        val parentHash = headerData["parent_root"].textValue()

        return BeaconChainBlockHeader(hash, parentHash, height)
    }
}

private data class BeaconChainSyncing(
    @JsonProperty("data")
    val data: BeaconChainSyncingData,
)

@JsonIgnoreProperties(ignoreUnknown = true)
private data class BeaconChainSyncingData(
    @JsonProperty("is_syncing")
    val isSyncing: Boolean,
)

private data class BeaconChainPeers(
    @JsonProperty("data")
    val data: BeaconChainPeersData,
)

@JsonIgnoreProperties(ignoreUnknown = true)
private data class BeaconChainPeersData(
    @JsonProperty("connected")
    val connected: String,
)
