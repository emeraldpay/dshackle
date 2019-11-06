/**
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
package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.ReactorBatch
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.net.ConnectException
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class UpstreamValidator(
        private val ethereumUpstream: EthereumUpstream,
        private val options: UpstreamsConfig.Options
) {
    companion object {
        private val log = LoggerFactory.getLogger(UpstreamValidator::class.java)
        val scheduler = Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("validator")))
    }

    fun validate(): Mono<UpstreamAvailability> {
        val batch = ReactorBatch()
        val peerCount = batch.add(Commands.net().peerCount()).result
        val syncing = batch.add(Commands.eth().syncing()).result
        return ethereumUpstream.getApi(Selector.empty)
                .subscribeOn(scheduler)
                .flatMapMany { api -> api.rpcClient.execute(batch) }
                .timeout(Defaults.timeout, Mono.error(Exception("Validation timeout")))
                .then(syncing)
                .flatMap { value ->
                    if (value.isSyncing) {
                        Mono.just(UpstreamAvailability.SYNCING)
                    } else {
                        peerCount.map { count ->
                            val minPeers = options.minPeers ?: 1
                            if (count < minPeers) {
                                UpstreamAvailability.IMMATURE
                            } else {
                                UpstreamAvailability.OK
                            }
                        }
                    }
                }
                .doOnError { err ->
                    if (ExceptionUtils.hasCause(err, ConnectException::class.java)) {
                        log.debug("Failed to connect to upstream: ${err.message}")
                    } else {
                        log.warn("Failed to validate upstream", err)
                    }
                }
                .onErrorReturn(UpstreamAvailability.UNAVAILABLE)
    }

    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(Duration.ofSeconds(15))
                .flatMap {
                    validate()
                }
    }
}