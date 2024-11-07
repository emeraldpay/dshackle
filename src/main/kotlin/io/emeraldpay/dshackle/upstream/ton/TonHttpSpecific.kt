package io.emeraldpay.dshackle.upstream.ton

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
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.generic.AbstractPollChainSpecific
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundService
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Instant

fun generateRandomString(length: Int): String {
    val allowedChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return (1..length)
        .map { allowedChars.random() }
        .joinToString("")
}

object TonHttpSpecific : AbstractPollChainSpecific() {
    override fun getFromHeader(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        throw NotImplementedError()
    }

    override fun listenNewHeadsRequest(): ChainRequest {
        throw NotImplementedError()
    }

    override fun unsubscribeNewHeadsRequest(subId: String): ChainRequest {
        throw NotImplementedError()
    }

    override fun lowerBoundService(chain: Chain, upstream: Upstream): LowerBoundService {
        return TonLowerBoundService(chain, upstream)
    }

    override fun latestBlockRequest(): ChainRequest {
        return ChainRequest("GET#/getMasterchainInfo", RestParams.emptyParams())
    }

    override fun parseBlock(data: ByteArray, upstreamId: String, api: ChainReader): Mono<BlockContainer> {
        val blockHeader = Global.objectMapper.readValue<TonMasterchainInfo>(data)

        return Mono.just(
            BlockContainer(
                height = blockHeader.result.last.seqno,
                hash = BlockId.fromBase64(blockHeader.result.last.root_hash),
                difficulty = BigInteger.ZERO,
                timestamp = Instant.EPOCH,
                full = false,
                json = data,
                parsed = blockHeader,
                transactions = emptyList(),
                upstreamId = upstreamId,
                parentHash = BlockId.fromBase64(generateRandomString(32)),
            ),
        )
    }

    override fun upstreamValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<UpstreamAvailability>> {
        return emptyList()
    }

    override fun upstreamSettingsValidators(
        chain: Chain,
        upstream: Upstream,
        options: Options,
        config: ChainConfig,
    ): List<SingleValidator<ValidateUpstreamSettingsResult>> {
        // add check generic block
        return listOf(
            TonChainIdValidator(upstream, chain),
        )
    }

    override fun chainSettingsValidator(
        chain: Chain,
        upstream: Upstream,
        reader: ChainReader?,
    ): SingleValidator<ValidateUpstreamSettingsResult>? {
        if (upstream.getOptions().disableUpstreamValidation) {
            return null
        }
        return TonChainIdValidator(upstream, chain)
    }
}

data class TonMasterchainInfoResultLast(
    val workchain: Int,
    val shard: String,
    val seqno: Long,
    val root_hash: String,
    val file_hash: String,
)

data class TonMasterchainInfoResult(
    val last: TonMasterchainInfoResultLast,
)

data class TonMasterchainInfo(
    val ok: Boolean,
    val result: TonMasterchainInfoResult,
)

class TonMasterchainInfoDeserializer : JsonDeserializer<TonMasterchainInfo>() {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): TonMasterchainInfo {
        val node = p.readValueAsTree<JsonNode>()
        val ok = node["ok"].booleanValue()
        val workchain = node["result"]["last"]["workchain"].intValue()
        val shard = node["result"]["last"]["shard"].textValue()
        val seqno = node["result"]["last"]["seqno"].longValue()
        val root_hash = node["result"]["last"]["root_hash"].textValue()
        val file_hash = node["result"]["last"]["file_hash"].textValue()

        return TonMasterchainInfo(
            ok,
            TonMasterchainInfoResult(
                TonMasterchainInfoResultLast(
                    workchain,
                    shard,
                    seqno,
                    root_hash,
                    file_hash,
                ),
            ),
        )
    }
}
