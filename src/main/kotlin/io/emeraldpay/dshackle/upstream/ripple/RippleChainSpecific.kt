package io.emeraldpay.dshackle.upstream.ripple

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
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant

object RippleChainSpecific : AbstractPollChainSpecific() {

    private val log = LoggerFactory.getLogger(RippleChainSpecific::class.java)

    override fun parseBlock(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val result = Global.objectMapper.readValue(data, RippleBlock::class.java)
        val block = result.closed.ledger
        var height: Long = 0
        try {
            height = block.ledgerIndex.toLong()
        } catch (e: NumberFormatException) {
            log.error("Invalid ledgerIndex $block.ledgerIndex, upstreamId:$upstreamId ", result)
        }

        return Mono.just(
            BlockContainer(
                height = height,
                hash = BlockId.from(block.ledgerHash ?: ""),
                difficulty = BigInteger.ZERO,
                timestamp = Instant.EPOCH,
                full = false,
                json = data,
                parsed = result,
                transactions = emptyList(),
                upstreamId = upstreamId,
                parentHash = BlockId.from(block.parentHash),
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
                ChainRequest("server_state", ListParams()),
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
                ChainRequest("server_state", ListParams()),
                upstream,
            ) { data ->
                validateSettings(data, chain)
            },
        )
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream): LowerBoundService {
        return RippleLowerBoundService(chain, upstream)
    }

    fun validate(data: ByteArray): UpstreamAvailability {
        val resp = Global.objectMapper.readValue(data, RippleState::class.java)
        return when (resp.state.serverState) {
            "full", "proposing" -> UpstreamAvailability.OK
            "connected" -> UpstreamAvailability.SYNCING
            else -> UpstreamAvailability.UNAVAILABLE
        }
    }

    fun validateSettings(data: ByteArray, chain: Chain): ValidateUpstreamSettingsResult {
        val resp = Global.objectMapper.readValue(data, RippleState::class.java)
        return if (chain.chainId.isNotEmpty() && resp.state.networkId.toString() != chain.chainId.lowercase()) {
            ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
        } else {
            ValidateUpstreamSettingsResult.UPSTREAM_VALID
        }
    }

    override fun latestBlockRequest(): ChainRequest =
        ChainRequest("ledger", ListParams())
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleBlock(
    @JsonProperty("closed") var closed: RippleClosed,
    @JsonProperty("open") var open: RippleOpen?,
    @JsonProperty("status ") var status: String?,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleClosed(
    @JsonProperty("ledger") var ledger: RippleClosedLedger,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleOpen(
    @JsonProperty("ledger") var ledger: RippleOpenLedger,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleClosedLedger(
    @JsonProperty("account_hash") var accountHash: String,
    @JsonProperty("close_flags") var closeFlags: Short,
    @JsonProperty("close_time") var closeTime: Long,
    @JsonProperty("close_time_human") var closeTimeHuman: String,
    @JsonProperty("close_time_iso") var closeTimeIso: String,
    @JsonProperty("close_time_resolution") var closeTimeResolution: Short,
    @JsonProperty("closed") var closed: Boolean,
    @JsonProperty("ledger_hash") var ledgerHash: String,
    @JsonProperty("ledger_index") var ledgerIndex: String,
    @JsonProperty("parent_close_time") var parentCloseTime: Long,
    @JsonProperty("parent_hash") var parentHash: String,
    @JsonProperty("total_coins") var totalCoins: String,
    @JsonProperty("transaction_hash") var transactionHash: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleOpenLedger(
    @JsonProperty("closed") var closed: Boolean,
    @JsonProperty("ledger_index") var ledgerIndex: String,
    @JsonProperty("parent_hash") var parentHash: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleState(
    @JsonProperty("state") var state: RippleServerState,
    @JsonProperty("status") var status: String? = null,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class RippleServerState(
    @JsonProperty("build_version") val buildVersion: String,
    @JsonProperty("complete_ledgers") val completeLedgers: String,
    @JsonProperty("initial_sync_duration_us") val initialSyncDurationUs: String,
    @JsonProperty("io_latency_ms") val ioLatencyMs: Int,
    @JsonProperty("jq_trans_overflow") val jqTransOverflow: String,
    @JsonProperty("last_close") val lastClose: LastClose,
    @JsonProperty("load_base") val loadBase: Int,
    @JsonProperty("load_factor") val loadFactor: Int,
    @JsonProperty("load_factor_fee_escalation") val loadFactorFeeEscalation: Int,
    @JsonProperty("load_factor_fee_queue") val loadFactorFeeQueue: Int,
    @JsonProperty("load_factor_fee_reference") val loadFactorFeeReference: Int,
    @JsonProperty("load_factor_server") val loadFactorServer: Int,
    @JsonProperty("network_id") val networkId: Int,
    @JsonProperty("peer_disconnects") val peerDisconnects: String,
    @JsonProperty("peer_disconnects_resources") val peerDisconnectsResources: String,
    @JsonProperty("peers") val peers: Int,
    @JsonProperty("ports") val ports: List<Port>,
    @JsonProperty("pubkey_node") val pubkeyNode: String,
    @JsonProperty("server_state") val serverState: String,
    @JsonProperty("server_state_duration_us") val serverStateDurationUs: String,
    @JsonProperty("state_accounting") val stateAccounting: StateAccounting,
    @JsonProperty("time") val time: String,
    @JsonProperty("uptime") val uptime: Long,
    @JsonProperty("validated_ledger") val validatedLedger: ValidatedLedger,
    @JsonProperty("validation_quorum") val validationQuorum: Int,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class LastClose(
    @JsonProperty("converge_time") val convergeTime: Int,
    @JsonProperty("proposers") val proposers: Int,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class Port(
    @JsonProperty("port") val port: String,
    @JsonProperty("protocol") val protocol: List<String>,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StateAccounting(
    @JsonProperty("connected") val connected: StateDuration,
    @JsonProperty("disconnected") val disconnected: StateDuration,
    @JsonProperty("full") val full: StateDuration,
    @JsonProperty("syncing") val syncing: StateDuration,
    @JsonProperty("tracking") val tracking: StateDuration,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StateDuration(
    @JsonProperty("duration_us") val durationUs: String,
    @JsonProperty("transitions") val transitions: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ValidatedLedger(
    @JsonProperty("base_fee") val baseFee: Int,
    @JsonProperty("close_time") val closeTime: Long,
    @JsonProperty("hash") val hash: String,
    @JsonProperty("reserve_base") val reserveBase: Long,
    @JsonProperty("reserve_inc") val reserveInc: Long,
    @JsonProperty("seq") val seq: Long,
)
