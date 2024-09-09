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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig.ChainConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.SingleValidator
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexData
import io.emeraldpay.dshackle.upstream.ethereum.json.TransactionCallJson
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.kotlin.extra.retry.retryRandomBackoff
import java.math.BigInteger
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.function.Supplier

interface CallLimitValidator : SingleValidator<ValidateUpstreamSettingsResult> {
    fun isEnabled(): Boolean
}

abstract class AbstractCallLimitValidator(
    private val upstream: Upstream,
    private val options: ChainOptions.Options,
) : CallLimitValidator {

    companion object {
        @JvmStatic
        val log: Logger = LoggerFactory.getLogger(AbstractCallLimitValidator::class.java)
    }
    override fun validate(onError: ValidateUpstreamSettingsResult): Mono<ValidateUpstreamSettingsResult> {
        return upstream.getIngressReader()
            .read(createRequest())
            .flatMap(ChainResponse::requireResult)
            .map { ValidateUpstreamSettingsResult.UPSTREAM_VALID }
            .onErrorResume {
                if (isLimitError(it)) {
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

    abstract fun createRequest(): ChainRequest

    abstract fun isLimitError(err: Throwable): Boolean
}

class EthCallLimitValidator(
    upstream: Upstream,
    private val options: ChainOptions.Options,
    private val config: ChainConfig,
) : AbstractCallLimitValidator(upstream, options) {
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
) : AbstractCallLimitValidator(upstream, options) {
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
        EthCallLimitValidator(upstream, options, config)
    }
}

class ChainIdValidator(
    private val upstream: Upstream,
    private val chain: Chain,
    private val customReader: ChainReader? = null,
) : SingleValidator<ValidateUpstreamSettingsResult> {
    private val validatorReader: Supplier<ChainReader> = Supplier {
        customReader ?: upstream.getIngressReader()
    }

    companion object {
        @JvmStatic
        val log: Logger = LoggerFactory.getLogger(ChainIdValidator::class.java)
    }
    override fun validate(onError: ValidateUpstreamSettingsResult): Mono<ValidateUpstreamSettingsResult> {
        return Mono.zip(
            chainId(),
            netVersion(),
        )
            .map {
                var netver: BigInteger
                if (it.t2.lowercase().contains("x")) {
                    netver = BigInteger(it.t2.lowercase().substringAfter("x"), 16)
                } else {
                    netver = BigInteger(it.t2)
                }
                val isChainValid = chain.chainId.lowercase() == it.t1.lowercase() &&
                    chain.netVersion == netver

                if (!isChainValid) {
                    val actualChain = Global.chainByChainId(it.t1).chainName
                    log.warn(
                        "${chain.chainName} is specified for upstream ${upstream.getId()} (chainId ${chain.chainId.lowercase()}, net_version ${chain.netVersion}) " +
                            "but actually it is $actualChain with chainId ${it.t1} and net_version $netver",
                    )
                }

                if (isChainValid) {
                    ValidateUpstreamSettingsResult.UPSTREAM_VALID
                } else {
                    ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
                }
            }
            .onErrorResume {
                log.error("Error during chain validation of upstream {}, reason - {}", upstream.getId(), it.message)
                Mono.just(onError)
            }
    }

    private fun chainId(): Mono<String> {
        return validatorReader.get()
            .read(ChainRequest("eth_chainId", ListParams()))
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during chainId retrieving for {}, iteration {}, reason - {}",
                    upstream.getId(),
                    ctx.iteration(),
                    ctx.exception().message,
                )
            }
            .doOnError { log.error("Error during execution 'eth_chainId' - {} for {}", it.message, upstream.getId()) }
            .flatMap(ChainResponse::requireStringResult)
    }

    private fun netVersion(): Mono<String> {
        return validatorReader.get()
            .read(ChainRequest("net_version", ListParams()))
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during netVersion retrieving for {}, iteration {}, reason - {}",
                    upstream.getId(),
                    ctx.iteration(),
                    ctx.exception().message,
                )
            }
            .doOnError { log.error("Error during execution 'net_version' - {} for {}", it.message, upstream.getId()) }
            .flatMap(ChainResponse::requireStringResult)
    }
}

class OldBlockValidator(
    private val upstream: Upstream,
) : SingleValidator<ValidateUpstreamSettingsResult> {

    companion object {
        @JvmStatic
        val log: Logger = LoggerFactory.getLogger(OldBlockValidator::class.java)
    }
    override fun validate(onError: ValidateUpstreamSettingsResult): Mono<ValidateUpstreamSettingsResult> {
        return EthereumArchiveBlockNumberReader(upstream.getIngressReader())
            .readArchiveBlock()
            .flatMap {
                upstream.getIngressReader()
                    .read(ChainRequest("eth_getBlockByNumber", ListParams(it, false)))
                    .flatMap(ChainResponse::requireResult)
            }
            .retryRandomBackoff(3, Duration.ofMillis(100), Duration.ofMillis(500)) { ctx ->
                log.warn(
                    "error during old block retrieving for {}, iteration {}, reason - {}",
                    upstream.getId(),
                    ctx.iteration(),
                    ctx.exception().message,
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
                log.warn("Error during old blocks validation of upstream {}, reason - {}", upstream.getId(), it.message)
                Mono.just(ValidateUpstreamSettingsResult.UPSTREAM_VALID)
            }
    }
}

class GasPriceValidator(
    private val upstream: Upstream,
    private val config: ChainConfig,
) : SingleValidator<ValidateUpstreamSettingsResult> {

    companion object {
        @JvmStatic
        val log: Logger = LoggerFactory.getLogger(GasPriceValidator::class.java)
    }
    override fun validate(onError: ValidateUpstreamSettingsResult): Mono<ValidateUpstreamSettingsResult> {
        if (!upstream.getMethods().isCallable("eth_gasPrice")) {
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
                Mono.just(onError)
            }
    }
}
