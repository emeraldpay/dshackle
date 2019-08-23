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
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.TimeUnit

class UpstreamValidator(
        private val ethereumUpstream: EthereumUpstream,
        private val options: UpstreamsConfig.Options
) {

    fun validate(): UpstreamAvailability {
        val batch = Batch()
        val peerCount = batch.add(Commands.net().peerCount())
        val syncing = batch.add(Commands.eth().syncing())
        try {
            ethereumUpstream.getApi(Selector.empty).rpcClient.execute(batch).get(5, TimeUnit.SECONDS)
            if (syncing.get().isSyncing) {
                return UpstreamAvailability.SYNCING
            }
            if (options.minPeers != null && peerCount.get() < options.minPeers!!) {
                return UpstreamAvailability.IMMATURE
            }
            return UpstreamAvailability.OK
        } catch (e: Throwable) {
            return UpstreamAvailability.UNAVAILABLE
        }
    }

    fun start(): Flux<UpstreamAvailability> {
        return Flux.interval(Duration.ofSeconds(15))
                .map {
                    validate()
                }.onErrorContinue { _, _ -> UpstreamAvailability.UNAVAILABLE }
    }
}