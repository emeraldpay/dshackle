package io.emeraldpay.dshackle.upstream.solana

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.BlockId
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.DefaultSolanaMethods
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.LabelsDetector
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.generic.AbstractChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericEgressSubscription
import io.emeraldpay.dshackle.upstream.generic.GenericIngressSubscription
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant

object SolanaChainSpecific : AbstractChainSpecific() {

    override fun getLatestBlock(api: JsonRpcReader, upstreamId: String): Mono<BlockContainer> {
        return api.read(JsonRpcRequest("getLatestBlockhash", listOf())).flatMap {
            val response = Global.objectMapper.readValue(it.getResult(), SolanaLatest::class.java)
            api.read(
                JsonRpcRequest(
                    "getBlock",
                    listOf(
                        response.context.slot,
                        mapOf(
                            "showRewards" to false,
                            "transactionDetails" to "none",
                            "maxSupportedTransactionVersion" to 0,
                        ),
                    ),
                ),
            ).map {
                val raw = it.getResult()
                val block = Global.objectMapper.readValue(it.getResult(), SolanaBlock::class.java)
                makeBlock(raw, block, upstreamId)
            }
        }
    }

    override fun parseHeader(data: ByteArray, upstreamId: String): BlockContainer {
        val res = Global.objectMapper.readValue(data, SolanaWrapper::class.java)
        return makeBlock(data, res.value.block, upstreamId)
    }

    private fun makeBlock(raw: ByteArray, block: SolanaBlock, upstreamId: String): BlockContainer {
        return BlockContainer(
            height = block.height,
            hash = BlockId.fromBase64(block.hash),
            difficulty = BigInteger.ZERO,
            timestamp = Instant.ofEpochMilli(block.timestamp),
            full = false,
            json = raw,
            parsed = block,
            transactions = emptyList(),
            upstreamId = upstreamId,
            parentHash = BlockId.fromBase64(block.parent),
        )
    }

    override fun listenNewHeadsRequest(): JsonRpcRequest {
        return JsonRpcRequest(
            "blockSubscribe",
            listOf(
                "all",
                mapOf(
                    "showRewards" to false,
                    "transactionDetails" to "none",
                ),
            ),
        )
    }

    override fun unsubscribeNewHeadsRequest(subId: String): JsonRpcRequest {
        return JsonRpcRequest("blockUnsubscribe", listOf(subId))
    }

    override fun labelDetector(chain: Chain, reader: JsonRpcReader): LabelsDetector? {
        return null
    }

    override fun makeIngressSubscription(ws: WsSubscriptions): IngressSubscription {
        return GenericIngressSubscription(ws)
    }

    override fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription {
        return { ms -> GenericEgressSubscription(ms, headScheduler, DefaultSolanaMethods.subs.map { it.first }) }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaLatest(
    @JsonProperty("context") var context: SolanaContext,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaWrapper(
    @JsonProperty("context") var context: SolanaContext,
    @JsonProperty("value") var value: SolanaResult,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaContext(
    @JsonProperty("slot") var slot: Long,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaResult(
    @JsonProperty("block") var block: SolanaBlock,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SolanaBlock(
    @JsonProperty("blockHeight") var height: Long,
    @JsonProperty("blockTime") var timestamp: Long,
    @JsonProperty("blockhash") var hash: String,
    @JsonProperty("previousBlockhash") var parent: String,
)
