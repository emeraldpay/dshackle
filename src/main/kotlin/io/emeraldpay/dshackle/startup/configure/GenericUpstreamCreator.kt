package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.IndexConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.hot.CompatibleVersionsRules
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.BlockValidator
import io.emeraldpay.dshackle.upstream.CallTargetsHolder
import io.emeraldpay.dshackle.upstream.forkchoice.NoChoiceWithPriorityForkChoice
import io.emeraldpay.dshackle.upstream.generic.ChainSpecificRegistry
import io.emeraldpay.dshackle.upstream.generic.GenericUpstream
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnectorFactory
import org.springframework.stereotype.Component
import java.util.function.Supplier

@Component
open class GenericUpstreamCreator(
    chainsConfig: ChainsConfig,
    indexConfig: IndexConfig,
    callTargets: CallTargetsHolder,
    private val connectorFactoryCreatorResolver: ConnectorFactoryCreatorResolver,
    private val versionRules: Supplier<CompatibleVersionsRules?>,
) : UpstreamCreator(chainsConfig, indexConfig, callTargets) {
    private val hashes = HashSet<Short>()

    override fun createUpstream(
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        options: ChainOptions.Options,
        chainConf: ChainsConfig.ChainConfig,
    ): UpstreamCreationData {
        return buildGenericUpstream(
            upstreamsConfig.nodeId,
            upstreamsConfig,
            upstreamsConfig.connection as UpstreamsConfig.RpcConnection,
            chain,
            options,
            chainConf,
            0,
        )
    }

    protected fun buildGenericUpstream(
        nodeId: Int?,
        config: UpstreamsConfig.Upstream<*>,
        connection: UpstreamsConfig.RpcConnection,
        chain: Chain,
        options: ChainOptions.Options,
        chainConfig: ChainsConfig.ChainConfig,
        nodeRating: Int,
    ): UpstreamCreationData {
        if (config.connection == null) {
            log.warn("Upstream doesn't have connection configuration")
            return UpstreamCreationData.default()
        }

        val cs = ChainSpecificRegistry.resolve(chain)

        val connectorFactory = connectorFactoryCreatorResolver.resolve(chain).createConnectorFactory(
            config.id!!,
            connection,
            chain,
            NoChoiceWithPriorityForkChoice(nodeRating, config.id!!),
            BlockValidator.ALWAYS_VALID,
            chainConfig,
        ) ?: return UpstreamCreationData.default()

        val hashUrl = connection.let {
            if (it.connectorMode == GenericConnectorFactory.ConnectorMode.RPC_REQUESTS_WITH_MIXED_HEAD.name) it.rpc?.url ?: it.ws?.url else it.ws?.url ?: it.rpc?.url
        }
        val hash = getHash(nodeId, hashUrl!!, hashes)
        val buildMethodsFun = { a: UpstreamsConfig.Upstream<*>, b: Chain -> this.buildMethods(a, b) }

        val upstream = GenericUpstream(
            config,
            chain,
            hash,
            options,
            QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels.fromMap(config.labels)),
            chainConfig,
            connectorFactory,
            cs::validator,
            cs::upstreamSettingsDetector,
            cs::upstreamRpcMethodsDetector,
            buildMethodsFun,
            cs::lowerBoundService,
            cs::finalizationDetectorBuilder,
            versionRules,
        )

        upstream.start()
        if (!upstream.isRunning) {
            log.debug("Upstream ${upstream.getId()} is not running, it can't be added")
            return UpstreamCreationData.default()
        }
        return UpstreamCreationData(upstream, upstream.isValid())
    }
}
