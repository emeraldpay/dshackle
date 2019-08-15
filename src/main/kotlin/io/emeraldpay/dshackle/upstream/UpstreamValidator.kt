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
            e.printStackTrace()
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