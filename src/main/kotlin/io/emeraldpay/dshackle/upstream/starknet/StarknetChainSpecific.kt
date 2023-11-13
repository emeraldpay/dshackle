package io.emeraldpay.dshackle.upstream.starknet

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import java.math.BigInteger
import java.time.Instant

object StarknetChainSpecific : AbstractPollChainSpecific() {
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

    override fun listenNewHeadsRequest(): JsonRpcRequest {
        throw NotImplementedError()
    }

    override fun unsubscribeNewHeadsRequest(subId: String): JsonRpcRequest {
        throw NotImplementedError()
    }

    override fun latestBlockRequest(): JsonRpcRequest =
        JsonRpcRequest("starknet_getBlockWithTxHashes", listOf("latest"))
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class StarknetBlock(
    @JsonProperty("block_hash") var hash: String,
    @JsonProperty("block_number") var number: Long,
    @JsonProperty("timestamp") var timestamp: Instant,
    @JsonProperty("parent_hash") var parent: String,
)
