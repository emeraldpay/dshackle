package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.foundation.ChainOptions.Options
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.ManagedCallMethods
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.function.Function
import kotlin.math.abs

abstract class UpstreamCreator(
    private val chainsConfig: ChainsConfig,
    private val callTargets: CallTargetsHolder,
) {
    protected val log: Logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        fun getHash(nodeId: Int?, obj: Any, hashes: MutableSet<Short>): Short {
            val hash = nodeId?.toShort()
                ?: run {
                    (obj.hashCode() % 65535)
                        .let { if (it == 0) 1 else it }
                        .let { nonZeroHash ->
                            listOf<Function<Int, Int>>(
                                Function { i -> i },
                                Function { i -> (-i) },
                                Function { i -> 32767 - abs(i) },
                                Function { i -> abs(i) - 32768 },
                            )
                                .map { it.apply(nonZeroHash).toShort() }
                                .firstOrNull { !hashes.contains(it) }
                        }
                        ?: (Short.MIN_VALUE..Short.MAX_VALUE).first {
                            it != 0 && !hashes.contains(it.toShort())
                        }.toShort()
                }

            return hash.also { hashes.add(it) }
        }
    }

    fun createUpstream(
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        defaultOptions: Map<Chain, ChainOptions.PartialOptions>,
    ): UpstreamCreationData {
        val chain = Global.chainById(upstreamsConfig.chain)
        if (chain == Chain.UNSPECIFIED) {
            throw IllegalArgumentException("Chain is unknown: ${upstreamsConfig.chain}")
        }
        val chainConfig = chainsConfig.resolve(upstreamsConfig.chain!!)
        val options = chainConfig.options
            .merge(defaultOptions[chain] ?: ChainOptions.PartialOptions.getDefaults())
            .merge(upstreamsConfig.options ?: ChainOptions.PartialOptions())
            .buildOptions()

        return createUpstream(upstreamsConfig, chain, options, chainConfig)
    }

    protected abstract fun createUpstream(
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        options: ChainOptions.Options,
        chainConf: ChainsConfig.ChainConfig,
    ): UpstreamCreationData

    protected fun buildMethods(config: UpstreamsConfig.Upstream<*>, chain: Chain, options: Options): CallMethods {
        return if (config.methods != null || config.methodGroups != null) {
            if (config.methodGroups == null) {
                config.methodGroups = UpstreamsConfig.MethodGroups(setOf("filter"), setOf())
            } else {
                val disabled = config.methodGroups!!.disabled
                if (!disabled.contains("filter")) {
                    config.methodGroups!!.enabled = config.methodGroups!!.enabled.plus("filter")
                }
            }

            ManagedCallMethods(
                delegate = callTargets.getDefaultMethods(chain, options, config.connection),
                enabled = config.methods?.enabled?.map { it.name }?.toSet() ?: emptySet(),
                disabled = config.methods?.disabled?.map { it.name }?.toSet() ?: emptySet(),
                groupsEnabled = config.methodGroups?.enabled ?: emptySet(),
                groupsDisabled = config.methodGroups?.disabled ?: emptySet(),
            ).also {
                config.methods?.enabled?.forEach { m ->
                    if (m.quorum != null) {
                        it.setQuorum(m.name, m.quorum)
                    }
                    if (m.static != null) {
                        it.setStaticResponse(m.name, m.static)
                    }
                }
            }
        } else {
            callTargets.getDefaultMethods(chain, options, config.connection)
        }
    }
}
