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
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.monitoring.record.RequestRecord
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.rpc.json.SyncingJson
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.function.Tuple2
import java.util.concurrent.Executors

open class EthereumUpstreamValidator(
    private val upstream: EthereumUpstream,
    private val options: UpstreamsConfig.Options
) {
    companion object {
        private val log = LoggerFactory.getLogger(EthereumUpstreamValidator::class.java)
        val scheduler =
            Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("ethereum-validator")))
    }

    private val objectMapper: ObjectMapper = Global.objectMapper

    open fun validate(): Mono<UpstreamAvailability> {
        return Mono.zip(
            validateSyncing(),
            validatePeers()
        )
            .map(::resolve)
            .defaultIfEmpty(UpstreamAvailability.UNAVAILABLE)
            .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun resolve(results: Tuple2<UpstreamAvailability, UpstreamAvailability>): UpstreamAvailability {
        return if (results.t1.isBetterTo(results.t2)) results.t2 else results.t1
    }

    fun validateSyncing(): Mono<UpstreamAvailability> {
        if (!options.validateSyncing) {
            return Mono.just(UpstreamAvailability.OK)
        }
        val request = JsonRpcRequest("eth_syncing", listOf())
        return upstream
            .getIngressReader()
            .read(request)
            .contextWrite(Global.monitoring.ingress.withBlockchain(upstream.getBlockchain()))
            .contextWrite(Global.monitoring.ingress.withRequest(request))
            .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.INTERNAL))
            .flatMap(JsonRpcResponse::requireResult)
            .map { objectMapper.readValue(it, SyncingJson::class.java) }
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for eth_syncing from ${upstream.getId()}") }
                    .then(Mono.error(SilentException.Timeout("Validation timeout for Syncing")))
            )
            .map { value ->
                if (value.isSyncing) {
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
        val request = JsonRpcRequest("net_peerCount", listOf())
        return upstream
            .getIngressReader()
            .read(request)
            .contextWrite(Global.monitoring.ingress.withRequest(request))
            .contextWrite(Global.monitoring.ingress.withBlockchain(upstream.getBlockchain()))
            .contextWrite(Global.monitoring.ingress.startCall(RequestRecord.Source.INTERNAL))
            .flatMap(JsonRpcResponse::requireStringResult)
            .map(Integer::decode)
            .timeout(
                Defaults.timeoutInternal,
                Mono.fromCallable { log.warn("No response for net_peerCount from ${upstream.getId()}") }
                    .then(Mono.error(SilentException.Timeout("Validation timeout for Peers")))
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
        return Flux.interval(options.validationInterval)
            .subscribeOn(scheduler)
            .flatMap {
                validate()
            }
    }
}
