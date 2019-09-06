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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstream
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

class UpstreamValidator(
        private val ethereumUpstream: EthereumUpstream,
        private val options: UpstreamsConfig.Options
) {

    fun validate(): Mono<UpstreamAvailability> {
        val batch = Batch()
        val peerCount = batch.add(Commands.net().peerCount())
        val syncing = batch.add(Commands.eth().syncing())
        return ethereumUpstream.getApi(Selector.empty)
                .map { api -> api.rpcClient.execute(batch) }
                .flatMap { Mono.fromCompletionStage(it) }
                .timeout(Duration.ofSeconds(10))
                .map {
                    if (syncing.get().isSyncing) {
                        UpstreamAvailability.SYNCING
                    } else if (options.minPeers != null && peerCount.get() < options.minPeers!!) {
                        UpstreamAvailability.IMMATURE
                    } else {
                        UpstreamAvailability.OK
                    }
                }.onErrorContinue { _, _ -> UpstreamAvailability.UNAVAILABLE }
    }

    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(Duration.ofSeconds(15))
                .flatMap {
                    validate()
                }
    }
}