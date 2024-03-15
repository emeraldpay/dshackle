package io.emeraldpay.dshackle.upstream.beaconchain

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.LabelsDetector
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import java.math.BigInteger
import java.time.Instant

object BeaconChainSpecific : AbstractPollChainSpecific() {
    override fun parseHeader(data: ByteArray, upstreamId: String): BlockContainer {
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

    override fun parseBlock(data: ByteArray, upstreamId: String): BlockContainer {
        val blockHeader = Global.objectMapper.readValue<BeaconChainBlockHeader>(data)

        return BlockContainer(
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
        )
    }

    override fun labelDetector(chain: Chain, reader: ChainReader): LabelsDetector {
        return BeaconChainLabelsDetector(reader)
    }

    override fun validator(
        chain: Chain,
        upstream: Upstream,
        options: ChainOptions.Options,
        config: ChainsConfig.ChainConfig,
    ): UpstreamValidator {
        return BeaconChainValidator(upstream, options)
    }

    override fun lowerBoundBlockDetector(chain: Chain, upstream: Upstream): LowerBoundBlockDetector {
        return BeaconChainLowerBoundBlockDetector(chain, upstream)
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
