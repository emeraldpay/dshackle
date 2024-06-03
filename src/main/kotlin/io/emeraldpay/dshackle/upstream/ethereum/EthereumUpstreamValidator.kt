/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.BasicEthUpstreamValidator
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData
import io.emeraldpay.dshackle.upstream.ethereum.json.SyncingJson
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionCallJson
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.extra.retry.retryRandomBackoff
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

open class EthereumUpstreamValidator @JvmOverloads constructor(
    private val chain: Chain,
    upstream: Upstream,
    options: ChainOptions.Options,
    private val config: ChainConfig,
) : BasicEthUpstreamValidator(upstream, options) {
    companion object {
        val scheduler =
            Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("ethereum-validator")))
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    override fun validateSyncingRequest(): ValidateSyncingRequest {
        return ValidateSyncingRequest(
            ChainRequest("eth_syncing", ListParams()),
        ) { bytes -> objectMapper.readValue(bytes, SyncingJson::class.java).isSyncing }
    }

    override fun validatePeersRequest(): ValidatePeersRequest {
        return ValidatePeersRequest(
            ChainRequest("net_peerCount", ListParams()),
        ) { resp -> Integer.decode(resp.getResultAsProcessedString()) }
    }

    override fun validatorFunctions(): List<Supplier<Mono<UpstreamAvailability>>> {
        return listOf(
            Supplier { validateSyncing() },
            Supplier { validatePeers() },
        )
    }

    override fun validateUpstreamSettings(): Mono<ValidateUpstreamSettingsResult> {
        if (options.disableUpstreamValidation) {
            return Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
        }
        return Mono.zip(
            validateChain(),
            validateOldBlocks(),
        ).map {
            listOf(it.t1, it.t2).maxOf { it }
        }
    }

    override fun validateUpstreamSettingsOnStartup(): ValidateUpstreamSettingsResult {
        if (options.disableUpstreamValidation) {
            return ValidateUpstreamSettingsResult.UPSTREAM_VALID
        }
        return Mono.zip(
            validateChain(),
            validateOldBlocks(),
            validateCallLimit(),
            validateGasPrice(),
        ).map {
            listOf(it.t1, it.t2, it.t3, it.t4).maxOf { it }
        }.block() ?: ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR
    }

    private fun validateChain(): Mono<ValidateUpstreamSettingsResult> {
        if (!options.validateChain) {
            return Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
        }
        return Mono.zip(
            chainId(),
            netVersion(),
        )
            .map {
                val isChainValid = chain.chainId.lowercase() == it.t1.lowercase() &&
                    chain.netVersion.toString() == it.t2

                if (!isChainValid) {
                    val actualChain = Global.chainByChainId(it.t1).chainName
                    log.warn(
                        "${chain.chainName} is specified for upstream ${upstream.getId()} " +
                            "but actually it is $actualChain with chainId ${it.t1} and net_version ${it.t2}",
                    )
                }

                if (isChainValid) {
                    ValidateUpstreamSettingsResult.UPSTREAM_VALID
                } else {
                    ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
                }
            }
            .onErrorResume {
                log.error("Error during chain validation", it)
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
            }
    }

    private fun validateCallLimit(): Mono<ValidateUpstreamSettingsResult> {
        val validator = callLimitValidatorFactory(upstream, options, config, chain)
        if (!validator.isEnabled()) {
            return Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
        }
        return upstream.getIngressReader()
            .read(validator.createRequest())
            .flatMap(ChainResponse::requireResult)
            .map { ValidateUpstreamSettingsResult.UPSTREAM_VALID }
            .onErrorResume {
                if (validator.isLimitError(it)) {
                    log.warn(
                        "Error: ${it.message}. Node ${upstream.getId()} is probably incorrectly configured. " +
                            "You need to set up your return limit to at least ${options.callLimitSize}. " +
                            "Erigon config example: https://github.com/ledgerwatch/erigon/blob/d014da4dc039ea97caf04ed29feb2af92b7b129d/cmd/utils/flags.go#L369",
                    )
                    Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR)
                } else {
                    Mono.error(it)
                }
            }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.error("No response for eth_call limit check from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for call limit"))),
            )
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during validateCallLimit for ${upstream.getId()}, iteration ${ctx.iteration()}, " +
                        "message ${ctx.exception().message}",
                )
            }
            .onErrorReturn(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
    }

    private fun validateOldBlocks(): Mono<ValidateUpstreamSettingsResult> {
        return EthereumArchiveBlockNumberReader(upstream.getIngressReader())
            .readArchiveBlock()
            .flatMap {
                upstream.getIngressReader()
                    .read(ChainRequest("eth_getBlockByNumber", ListParams(it, false)))
                    .flatMap(ChainResponse::requireResult)
            }
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during old block retrieving for ${upstream.getId()}, iteration ${ctx.iteration()}, " +
                        "message ${ctx.exception().message}",
                )
            }
            .map { result ->
                val receivedResult = result.isNotEmpty() && !Global.nullValue.contentEquals(result)
                if (!receivedResult) {
                    log.warn(
                        "Node ${upstream.getId()} probably is synced incorrectly, it is not possible to get old blocks",
                    )
                }
                ValidateUpstreamSettingsResult.UPSTREAM_VALID
            }
            .onErrorResume {
                log.warn("Error during old blocks validation", it)
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
            }
    }

    private fun validateGasPrice(): Mono<ValidateUpstreamSettingsResult> {
        if (!options.validateGasPrice) {
            return Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
        }
        return upstream.getIngressReader()
            .read(ChainRequest("eth_gasPrice", ListParams()))
            .flatMap(ChainResponse::requireStringResult)
            .map { result ->
                val actualGasPrice = result.substring(2).toLong(16)
                if (!config.gasPriceCondition.check(actualGasPrice)) {
                    log.warn(
                        "Node ${upstream.getId()} has gasPrice $actualGasPrice, " +
                            "but it is not equal to the required ${config.gasPriceCondition.rules()}",
                    )
                    ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
                } else {
                    ValidateUpstreamSettingsResult.UPSTREAM_VALID
                }
            }
            .onErrorResume { err ->
                log.warn("Error during gasPrice validation", err)
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR)
            }
    }

    private fun chainId(): Mono<String> {
        return upstream.getIngressReader()
            .read(ChainRequest("eth_chainId", ListParams()))
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during chainId retrieving for ${upstream.getId()}, iteration ${ctx.iteration()}, " +
                        "message ${ctx.exception().message}",
                )
            }
            .doOnError { log.error("Error during execution 'eth_chainId' - ${it.message} for ${upstream.getId()}") }
            .flatMap(ChainResponse::requireStringResult)
    }

    private fun netVersion(): Mono<String> {
        return upstream.getIngressReader()
            .read(ChainRequest("net_version", ListParams()))
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during netVersion retrieving for ${upstream.getId()}, iteration ${ctx.iteration()}, " +
                        "message ${ctx.exception().message}",
                )
            }
            .doOnError { log.error("Error during execution 'net_version' - ${it.message} for ${upstream.getId()}") }
            .flatMap(ChainResponse::requireStringResult)
    }
}

interface CallLimitValidator {
    fun isEnabled(): Boolean
    fun createRequest(): ChainRequest
    fun isLimitError(err: Throwable): Boolean
}

class EthCallLimitValidator(
    private val options: ChainOptions.Options,
    private val config: ChainConfig,
) : CallLimitValidator {
    override fun isEnabled() = options.validateCallLimit && config.callLimitContract != null

    override fun createRequest() = ChainRequest(
        "eth_call",
        ListParams(
            TransactionCallJson(
                Address.from(config.callLimitContract),
                // contract like https://github.com/drpcorg/dshackle/pull/246
                // meta + size in hex
                HexData.from("0xd8a26e3a" + options.callLimitSize.toString(16).padStart(64, '0')),
            ),
            "latest",
        ),
    )

    override fun isLimitError(err: Throwable): Boolean =
        err.message != null && err.message!!.contains("rpc.returndata.limit")
}

class ZkSyncCallLimitValidator(
    private val upstream: Upstream,
    private val options: ChainOptions.Options,
) : CallLimitValidator {
    private val method = "debug_traceBlockByNumber"

    override fun isEnabled() =
        options.validateCallLimit && upstream.getMethods().getSupportedMethods().contains(method)

    override fun createRequest() = ChainRequest(
        method,
        ListParams("0x1b73b2b", mapOf("tracer" to "callTracer")),
    )

    override fun isLimitError(err: Throwable): Boolean =
        err.message != null && err.message!!.contains("response size should not greater than")
}

fun callLimitValidatorFactory(
    upstream: Upstream,
    options: ChainOptions.Options,
    config: ChainConfig,
    chain: Chain,
): CallLimitValidator {
    return if (listOf(Chain.ZKSYNC__MAINNET).contains(chain)) {
        ZkSyncCallLimitValidator(upstream, options)
    } else {
        EthCallLimitValidator(options, config)
    }
}
