package io.emeraldpay.dshackle.config.reload

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.ConfiguredUpstreams
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
open class ReloadConfigUpstreamService(
    private val eventPublisher: ApplicationEventPublisher,
    private val multistreamHolder: CurrentMultistreamHolder,
    private val configuredUpstreams: ConfiguredUpstreams,
) {

    fun reloadUpstreams(
        chainsToReload: Set<Chain>,
        upstreamsToRemove: List<Pair<String, Chain>>,
        upstreamsToAdd: Set<String>,
        newUpstreamsConfig: UpstreamsConfig,
    ) {
        val usedChains = removeUpstreams(chainsToReload, upstreamsToRemove)

        addUpstreams(newUpstreamsConfig, chainsToReload, upstreamsToAdd)

        usedChains.forEach {
            multistreamHolder.getUpstream(it)
                .run {
                    if (!this.haveUpstreams() && this.isRunning()) {
                        this.stop()
                    }
                }
        }
    }

    private fun removeUpstreams(
        chainsToReload: Set<Chain>,
        upstreamsToRemove: List<Pair<String, Chain>>,
    ): Set<Chain> {
        val usedChains = mutableSetOf<Chain>()

        chainsToReload.forEach {
            usedChains.add(it)
            multistreamHolder.getUpstream(it)
                .getAll()
                .forEach { up ->
                    eventPublisher.publishEvent(UpstreamChangeEvent(it, up, UpstreamChangeEvent.ChangeType.REMOVED))
                }
        }
        upstreamsToRemove.forEach { pair ->
            usedChains.add(pair.second)
            multistreamHolder.getUpstream(pair.second)
                .getAll()
                .find { pair.first == it.getId() }
                ?.let {
                    eventPublisher.publishEvent(UpstreamChangeEvent(pair.second, it, UpstreamChangeEvent.ChangeType.REMOVED))
                }
        }

        return usedChains
    }

    private fun addUpstreams(
        newUpstreamsConfig: UpstreamsConfig,
        chainsToReload: Set<Chain>,
        upstreamsToAdd: Set<String>,
    ) {
        val configToReload = UpstreamsConfig(
            newUpstreamsConfig.defaultOptions,
            newUpstreamsConfig.upstreams.filter {
                chainsToReload.contains(Global.chainById(it.chain)) || upstreamsToAdd.contains(it.id)
            }.toMutableList(),
        )
        configuredUpstreams.processUpstreams(configToReload)
    }
}
