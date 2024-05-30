package io.emeraldpay.dshackle.upstream.solana

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
import io.emeraldpay.dshackle.upstream.DefaultSolanaMethods
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.SingleCallValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.generic.AbstractChainSpecific
import io.emeraldpay.dshackle.upstream.generic.GenericEgressSubscription
import io.emeraldpay.dshackle.upstream.generic.GenericIngressSubscription
import io.emeraldpay.dshackle.upstream.generic.GenericUpstreamValidator
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.math.BigInteger
import java.time.Instant

object SolanaChainSpecific : AbstractChainSpecific() {

    private val log = LoggerFactory.getLogger(SolanaChainSpecific::class.java)

    override fun getLatestBlock(api: ChainReader, upstreamId: String): Mono<BlockContainer> {
        return api.read(ChainRequest("getSlot", ListParams())).flatMap {
            val slot = it.getResultAsProcessedString().toLong()
            api.read(
                ChainRequest(
                    "getBlocks",
                    ListParams(
                        slot - 10,
                        slot,
                    ),
                ),
            ).flatMap {
                val response = Global.objectMapper.readValue(it.getResult(), LongArray::class.java)
                if (response == null || response.isEmpty()) {
                    Mono.empty()
                } else {
                    api.read(
                        ChainRequest(
                            "getBlock",
                            ListParams(
                                response.max(),
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
                        makeBlock(raw, block, upstreamId, response.max())
                    }.onErrorResume {
                        log.debug("error during getting last solana block - ${it.message}")
                        Mono.empty()
                    }
                }
            }
        }
    }

    override fun getFromHeader(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val res = Global.objectMapper.readValue(data, SolanaWrapper::class.java)
        return Mono.just(makeBlock(data, res.value.block, upstreamId, res.context.slot))
    }

    private fun makeBlock(raw: ByteArray, block: SolanaBlock, upstreamId: String, slot: Long): BlockContainer {
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
            slot = slot,
        )
    }

    override fun listenNewHeadsRequest(): ChainRequest {
        return ChainRequest(
            "blockSubscribe",
            ListParams(
                "all",
                mapOf(
                    "showRewards" to false,
                    "transactionDetails" to "none",
                ),
            ),
        )
    }

    override fun unsubscribeNewHeadsRequest(subId: String): ChainRequest {
        return ChainRequest("blockUnsubscribe", ListParams(subId))
    }

    override fun validator(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): UpstreamValidator {
        return GenericUpstreamValidator(
            upstream,
            options,
            SingleCallValidator(
                ChainRequest("getHealth", ListParams()),
            ) { data ->
                val resp = String(data)
                if (resp == "\"ok\"") {
                    UpstreamAvailability.OK
                } else {
                    log.warn("Upstream {} validation failed, solana status is {}", upstream.getId(), resp)
                    UpstreamAvailability.UNAVAILABLE
                }
            },
        )
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream): LowerBoundService {
        return SolanaLowerBoundService(chain, upstream)
    }

    override fun upstreamSettingsDetector(chain: Chain, upstream: Upstream): UpstreamSettingsDetector {
        return SolanaUpstreamSettingsDetector(upstream)
    }

    override fun makeIngressSubscription(ws: WsSubscriptions): IngressSubscription {
        return GenericIngressSubscription(ws, DefaultSolanaMethods.subs.map { it.first })
    }

    override fun subscriptionBuilder(headScheduler: Scheduler): (Multistream) -> EgressSubscription {
        return { ms -> GenericEgressSubscription(ms, headScheduler) }
    }
}

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
