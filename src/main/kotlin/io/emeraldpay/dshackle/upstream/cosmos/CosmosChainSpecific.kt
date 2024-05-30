package io.emeraldpay.dshackle.upstream.cosmos

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
import io.emeraldpay.dshackle.upstream.SingleCallValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability.OK
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericUpstreamValidator
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import io.emeraldpay.dshackle.upstream.rpcclient.ObjectParams
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant

object CosmosChainSpecific : AbstractPollChainSpecific() {
    override fun latestBlockRequest(): ChainRequest = ChainRequest("block", ObjectParams())

    override fun parseBlock(data: ByteArray, upstreamId: String): BlockContainer {
        val result = Global.objectMapper.readValue(data, CosmosBlockResult::class.java)

        return BlockContainer(
            height = result.block.header.height.toLong(),
            hash = BlockId.from(result.blockId.hash),
            difficulty = BigInteger.ZERO,
            timestamp = result.block.header.time,
            full = false,
            json = data,
            parsed = result,
            transactions = emptyList(),
            upstreamId = upstreamId,
            parentHash = BlockId.from(result.block.header.lastBlockId.hash),
        )
    }

    override fun getFromHeader(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val event = Global.objectMapper.readValue(data, CosmosBlockEvent::class.java)

        return api.read(ChainRequest("block", ObjectParams("height" to event.data.value.header.height))).flatMap {
            val blockData = it.getResult()
            val result = Global.objectMapper.readValue(blockData, CosmosBlockResult::class.java)
            Mono.just(
                BlockContainer(
                    height = result.block.header.height.toLong(),
                    hash = BlockId.from(result.blockId.hash),
                    difficulty = BigInteger.ZERO,
                    timestamp = result.block.header.time,
                    full = false,
                    json = blockData,
                    parsed = result,
                    transactions = emptyList(),
                    upstreamId = upstreamId,
                    parentHash = BlockId.from(result.block.header.lastBlockId.hash),
                ),
            )
        }
    }

    override fun listenNewHeadsRequest() = throw NotImplementedError()
    // ChainRequest("subscribe", ListParams("tm.event = 'NewBlockHeader'"))

    override fun unsubscribeNewHeadsRequest(subId: String) = throw NotImplementedError()
    // ChainRequest("unsubscribe", ListParams("tm.event = 'NewBlockHeader'"))

    override fun validator(chain: Chain, upstream: Upstream, options: Options, config: ChainConfig): UpstreamValidator {
        return GenericUpstreamValidator(
            upstream,
            options,
            SingleCallValidator(
                ChainRequest("health", ListParams()),
            ) { _ -> OK },
        )
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream) =
        CosmosLowerBoundService(chain, upstream)

    override fun upstreamSettingsDetector(chain: Chain, upstream: Upstream) =
        CosmosUpstreamSettingsDetector(upstream)
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosBlockResult(
    @JsonProperty("block_id") var blockId: CosmosBlockId,
    @JsonProperty("block") var block: CosmosBlockData,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosBlockId(
    @JsonProperty("hash") var hash: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosHeader(
    @JsonProperty("last_block_id") var lastBlockId: CosmosBlockId,
    @JsonProperty("height") var height: String,
    @JsonProperty("time") var time: Instant,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosStatus(
    @JsonProperty("node_info") var nodeInfo: CosmosNodeInfo,
    @JsonProperty("sync_info") var syncInfo: CosmosSyncInfo,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosNodeInfo(
    @JsonProperty("version") var version: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosSyncInfo(
    @JsonProperty("earliest_block_height") var earliestBlockHeight: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosBlockEvent(
    @JsonProperty("data") var data: CosmosBlockEventData,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class CosmosBlockEventData(
    @JsonProperty("value") var value: CosmosBlockData,
)

data class CosmosBlockData(
    @JsonProperty("header") var header: CosmosHeader,
)
