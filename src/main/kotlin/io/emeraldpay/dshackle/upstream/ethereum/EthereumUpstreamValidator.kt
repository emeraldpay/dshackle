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
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.infinitape.etherjar.rpc.json.SyncingJson
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors

class EthereumUpstreamValidator(
        private val upstream: EthereumUpstream,
        private val options: UpstreamsConfig.Options,
        private val objectMapper: ObjectMapper
) {
    companion object {
        private val log = LoggerFactory.getLogger(EthereumUpstreamValidator::class.java)
        val scheduler = Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("ethereum-validator")))
    }

    fun validate(): Mono<UpstreamAvailability> {
        return upstream
                .getApi()
                .read(JsonRpcRequest("eth_syncing", listOf()))
                .flatMap(JsonRpcResponse::requireResult)
                .map { objectMapper.readValue(it, SyncingJson::class.java) }
                .timeout(Defaults.timeoutInternal, Mono.error(Exception("Validation timeout for Syncing")))
                .flatMap { value ->
                    if (value.isSyncing) {
                        Mono.just(UpstreamAvailability.SYNCING)
                    } else {
                        upstream
                                .getApi()
                                .read(JsonRpcRequest("net_peerCount", listOf()))
                                .flatMap(JsonRpcResponse::requireStringResult)
                                .map(Integer::decode)
                                .timeout(Defaults.timeoutInternal, Mono.error(Exception("Validation timeout for Peers")))
                                .map { count ->
                                    val minPeers = options.minPeers ?: 1
                                    if (count < minPeers) {
                                        UpstreamAvailability.IMMATURE
                                    } else {
                                        UpstreamAvailability.OK
                                    }
                                }
                    }
                }
                .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(Duration.ofSeconds(15))
                .subscribeOn(scheduler)
                .flatMap {
                    validate()
                }
    }
}