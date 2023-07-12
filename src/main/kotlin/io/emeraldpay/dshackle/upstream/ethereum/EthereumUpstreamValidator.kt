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
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.rpc.json.SyncingJson
import io.emeraldpay.etherjar.rpc.json.TransactionCallJson
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.function.Tuple3
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException

open class EthereumUpstreamValidator @JvmOverloads constructor(
    private val upstream: Upstream,
    private val options: UpstreamsConfig.Options,
    private val callLimitContract: String? = null
) {
    private var callLimitSucceed: Boolean = false
    companion object {
        private val log = LoggerFactory.getLogger(EthereumUpstreamValidator::class.java)
        val scheduler =
            Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("ethereum-validator")))
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    open fun validate(): Mono<UpstreamAvailability> {
        return Mono.zip(
            validateSyncing(),
            validatePeers(),
            validateCallLimit()
        )
            .map(::resolve)
            .defaultIfEmpty(UpstreamAvailability.UNAVAILABLE)
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun resolve(results: Tuple3<UpstreamAvailability, UpstreamAvailability, UpstreamAvailability>): UpstreamAvailability {
        val cp = Comparator { avail1: UpstreamAvailability, avail2: UpstreamAvailability -> if (avail1.isBetterTo(avail2)) -1 else 1 }
        return listOf(results.t1, results.t2, results.t3).sortedWith(cp).last()
    }

    fun validateCallLimit(): Mono<UpstreamAvailability> {
        // do not rerun this check after first success because it's more expensive than others
        if (!options.validateCallLimit || callLimitContract == null || callLimitSucceed) {
            return Mono.just(UpstreamAvailability.OK)
        }
        return upstream.getIngressReader()
            .read(
                JsonRpcRequest(
                    "eth_call",
                    listOf(
                        TransactionCallJson(
                            Address.from(callLimitContract),
                            // calling contract with param 200_000, meaning it will generate 200k symbols or response
                            // 30ce0 + metadata — 200_000k
                            HexData.from("0xd8a26e3a0000000000000000000000000000000000000000000000000000000000030ce0")
                        ),
                        "latest"
                    )
                )
            )
            .flatMap(JsonRpcResponse::requireResult)
            .doOnError {
                log.error(
                    "Error: ${it.message}. Node ${upstream.getId()} is prbably incorrectly configured. " +
                        "You need to set up your return limit to at least 200000. " +
                        "Erigon config example: https://github.com/ledgerwatch/erigon/blob/devel/cmd/utils/flags.go#L364. "
                )
            }
            .map { UpstreamAvailability.OK }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.error("No response for eth_call limit check from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for call limit")))
            )
            .doOnSuccess { callLimitSucceed = true }
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun validateSyncing(): Mono<UpstreamAvailability> {
        if (!options.validateSyncing) {
            return Mono.just(UpstreamAvailability.OK)
        }
        return upstream.getIngressReader()
            .read(JsonRpcRequest("eth_syncing", listOf()))
            .flatMap(JsonRpcResponse::requireResult)
            .map { objectMapper.readValue(it, SyncingJson::class.java) }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for eth_syncing from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for Syncing")))
            )
            .map {
                val isSyncing = it.isSyncing
                upstream.getHead().onSyncingNode(isSyncing)
                if (isSyncing) {
                    UpstreamAvailability.SYNCING
                } else {
                    UpstreamAvailability.OK
                }
            }
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun validatePeers(): Mono<UpstreamAvailability> {
        if (!options.validatePeers || options.minPeers == 0) {
            return Mono.just(UpstreamAvailability.OK)
        }
        return upstream
            .getIngressReader()
            .read(JsonRpcRequest("net_peerCount", listOf()))
            .flatMap(JsonRpcResponse::requireStringResult)
            .map(Integer::decode)
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for net_peerCount from ${upstream.getId()}") }
                    .then(Mono.error(TimeoutException("Validation timeout for Peers")))
            )
            .map { count ->
                val minPeers = options.minPeers ?: 1
                if (count < minPeers) {
                    UpstreamAvailability.IMMATURE
                } else {
                    UpstreamAvailability.OK
                }
            }
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(
            Duration.ZERO,
            Duration.ofSeconds(options.validationInterval.toLong()),
        ).subscribeOn(scheduler)
            .flatMap {
                validate()
            }
    }
}
