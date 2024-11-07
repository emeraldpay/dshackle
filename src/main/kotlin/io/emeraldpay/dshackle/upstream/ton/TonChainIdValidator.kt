package io.emeraldpay.dshackle.upstream.ton

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class TonChainIdValidator(
    private val upstream: Upstream,
    private val chain: Chain,
) : SingleValidator<ValidateUpstreamSettingsResult> {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(TonChainIdValidator::class.java)
    }

    override fun validate(onError: ValidateUpstreamSettingsResult): Mono<ValidateUpstreamSettingsResult> {
        return upstream.getIngressReader()
            .read(TonHttpSpecific.latestBlockRequest())
            .flatMap(ChainResponse::requireResult)
            .flatMap {
                val tonMasterchainInfo = Global.objectMapper.readValue<TonMasterchainInfo>(it)

                upstream.getIngressReader()
                    .read(
                        ChainRequest(
                            "GET#/getBlockHeader",
                            RestParams(
                                emptyList(),
                                listOf(
                                    "workchain" to tonMasterchainInfo.result.last.workchain.toString(),
                                    "shard" to tonMasterchainInfo.result.last.shard,
                                    "seqno" to tonMasterchainInfo.result.last.seqno.toString(),
                                ),
                                emptyList(),
                                ByteArray(0),
                            ),
                        ),
                    )
                    .flatMap(ChainResponse::requireResult)
                    .flatMap { headerResponse ->
                        val blockHeader = Global.objectMapper.readValue<JsonNode>(headerResponse)

                        val globalId = blockHeader.get("result")?.get("global_id")?.asText()
                        if (globalId == null) {
                            log.warn(
                                "Couldn't receive global_id from the block header of upstream {} for workchain {}, shard {}, seqno {}",
                                upstream.getId(),
                                tonMasterchainInfo.result.last.workchain,
                                tonMasterchainInfo.result.last.shard,
                                tonMasterchainInfo.result.last.seqno,
                            )
                            Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
                        } else {
                            if (globalId == chain.chainId) {
                                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
                            } else {
                                log.warn(
                                    "Wrong chainId {} for {}, expected {}",
                                    globalId,
                                    chain,
                                    chain.chainId,
                                )
                                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR)
                            }
                        }
                    }
            }.onErrorResume {
                log.error("Error during chain validation of upstream {}, reason - {}", upstream.getId(), it.message)
                Mono.just(onError)
            }
    }
}
