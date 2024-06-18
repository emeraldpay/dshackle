package io.emeraldpay.dshackle.config.reload

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global.Companion.chainById
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import sun.misc.Signal
import sun.misc.SignalHandler
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Collectors

@Component
class ReloadConfigSetup(
    private val reloadConfigService: ReloadConfigService,
    private val reloadConfigUpstreamService: ReloadConfigUpstreamService,
) : SignalHandler {

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java)
        private val signalHup = Signal("HUP")
    }

    private val reloadLock = ReentrantLock()

    init {
        Signal.handle(signalHup, this)
    }

    override fun handle(sig: Signal) {
        if (sig == signalHup) {
            try {
                handle()
            } catch (e: Exception) {
                log.warn("Config is not reloaded, cause - ${e.message}", e)
            }
        }
    }

    private fun handle() {
        if (reloadLock.tryLock()) {
            try {
                log.info("Reloading config...")

                if (reloadConfig()) {
                    log.info("Config is reloaded")
                } else {
                    log.info("There is nothing to reload, config is the same")
                }
            } finally {
                reloadLock.unlock()
            }
        } else {
            log.warn("Reloading is in progress")
        }
    }

    private fun reloadConfig(): Boolean {
        val newUpstreamsConfig = reloadConfigService.readUpstreamsConfig()
        val currentUpstreamsConfig = reloadConfigService.currentUpstreamsConfig()

        if (newUpstreamsConfig == currentUpstreamsConfig) {
            return false
        }

        val chainsToReload = analyzeDefaultOptions(
            currentUpstreamsConfig.defaultOptions,
            newUpstreamsConfig.defaultOptions,
        )
        val upstreamsAnalyzeData = analyzeUpstreams(
            currentUpstreamsConfig.upstreams,
            newUpstreamsConfig.upstreams,
        )

        val upstreamsToRemove = upstreamsAnalyzeData.removed
            .filterNot { chainsToReload.contains(it.second) }
            .toSet()
        val upstreamsToAdd = upstreamsAnalyzeData.added

        reloadConfigService.updateUpstreamsConfig(newUpstreamsConfig)

        reloadConfigUpstreamService.reloadUpstreams(chainsToReload, upstreamsToRemove, upstreamsToAdd, newUpstreamsConfig)

        return true
    }

    private fun analyzeUpstreams(
        currentUpstreams: List<UpstreamsConfig.Upstream<*>>,
        newUpstreams: List<UpstreamsConfig.Upstream<*>>,
    ): UpstreamAnalyzeData {
        if (currentUpstreams == newUpstreams) {
            return UpstreamAnalyzeData()
        }
        val reloaded = mutableSetOf<Pair<String, Chain>>()
        val removed = mutableSetOf<Pair<String, Chain>>()
        val currentUpstreamsMap = currentUpstreams.associateBy { it.id!! to chainById(it.chain) }
        val newUpstreamsMap = newUpstreams.associateBy { it.id!! to chainById(it.chain) }

        currentUpstreamsMap.forEach {
            val newUpstream = newUpstreamsMap[it.key]
            if (newUpstream == null) {
                removed.add(it.key)
            } else if (newUpstream != it.value) {
                reloaded.add(it.key)
            }
        }

        val added = newUpstreamsMap
            .minus(currentUpstreamsMap.keys)
            .mapTo(mutableSetOf()) { it.key }
            .plus(reloaded)

        return UpstreamAnalyzeData(added, removed.plus(reloaded))
    }

    private fun analyzeDefaultOptions(
        currentDefaultOptions: List<ChainOptions.DefaultOptions>,
        newDefaultOptions: List<ChainOptions.DefaultOptions>,
    ): Set<Chain> {
        val chainsToReload = mutableSetOf<Chain>()

        val currentOptions = getChainOptions(currentDefaultOptions)
        val newOptions = getChainOptions(newDefaultOptions)

        if (currentOptions == newOptions) {
            return emptySet()
        }

        val removed = mutableSetOf<Chain>()

        currentOptions.forEach {
            val newChainOption = newOptions[it.key]
            if (newChainOption == null) {
                removed.add(chainById(it.key))
            } else if (newChainOption != it.value) {
                chainsToReload.add(chainById(it.key))
            }
        }

        val added = newOptions.minus(currentOptions.keys).map { chainById(it.key) }

        return chainsToReload.plus(added).plus(removed)
    }

    private fun getChainOptions(
        defaultOptions: List<ChainOptions.DefaultOptions>,
    ): Map<String, List<ChainOptions.PartialOptions>> {
        return defaultOptions.stream()
            .flatMap { options -> options.chains?.stream()?.map { it to options.options } }
            .collect(
                Collectors.groupingBy(
                    { it.first },
                    Collectors.mapping(
                        { it.second },
                        Collectors.toUnmodifiableList(),
                    ),
                ),
            )
    }

    private data class UpstreamAnalyzeData(
        val added: Set<Pair<String, Chain>> = emptySet(),
        val removed: Set<Pair<String, Chain>> = emptySet(),
    )
}
