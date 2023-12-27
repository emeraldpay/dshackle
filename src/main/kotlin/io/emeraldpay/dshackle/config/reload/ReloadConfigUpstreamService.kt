package io.emeraldpay.dshackle.config.reload

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.Global.Companion.chainById
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.ConfiguredUpstreams
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.CurrentMultistreamHolder
import org.springframework.stereotype.Component
import java.util.stream.Collectors

@Component
open class ReloadConfigUpstreamService(
    private val multistreamHolder: CurrentMultistreamHolder,
    private val configuredUpstreams: ConfiguredUpstreams,
) {

    fun reloadUpstreams(
        chainsToReload: Set<Chain>,
        upstreamsToRemove: Set<Pair<String, Chain>>,
        upstreamsToAdd: Set<Pair<String, Chain>>,
        newUpstreamsConfig: UpstreamsConfig,
    ) {
        val newUpstreamsCount = newUpstreamsConfig.upstreams.stream()
            .collect(
                Collectors.groupingBy(
                    { chainById(it.chain) },
                    Collectors.counting(),
                ),
            )

        val usedChains = removeUpstreams(chainsToReload, upstreamsToRemove)

        addUpstreams(newUpstreamsConfig, chainsToReload, upstreamsToAdd.map { it.first }.toSet())

        usedChains.forEach { chain ->
            if (newUpstreamsCount[chain] == null) {
                multistreamHolder.getUpstream(chain).stop()
            }
        }
    }

    private fun removeUpstreams(
        chainsToReload: Set<Chain>,
        upstreamsToRemove: Set<Pair<String, Chain>>,
    ): Set<Chain> {
        val usedChains = mutableSetOf<Chain>()

        chainsToReload.forEach {
            usedChains.add(it)
            val ms = multistreamHolder.getUpstream(it)
            ms.getAll()
                .forEach { up ->
                    ms.processUpstreamsEvents(UpstreamChangeEvent(it, up, UpstreamChangeEvent.ChangeType.REMOVED))
                }
        }
        upstreamsToRemove.forEach { pair ->
            usedChains.add(pair.second)
            val ms = multistreamHolder.getUpstream(pair.second)
            ms.getAll()
                .find { pair.first == it.getId() }
                ?.let { up ->
                    ms.processUpstreamsEvents(UpstreamChangeEvent(pair.second, up, UpstreamChangeEvent.ChangeType.REMOVED))
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
