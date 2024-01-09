package io.emeraldpay.dshackle.startup.configure

import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.upstream.Upstream
import org.springframework.stereotype.Component

data class UpstreamCreationData(
    val upstream: Upstream?,
    val isValid: Boolean,
) {
    companion object {
        fun default() = UpstreamCreationData(null, false)
    }
}

@Component
class UpstreamFactory(
    private val genericUpstreamCreator: GenericUpstreamCreator,
    private val ethereumUpstreamCreator: EthereumUpstreamCreator,
    private val bitcoinUpstreamCreator: BitcoinUpstreamCreator,
) {

    fun createUpstream(
        type: BlockchainType,
        upstreamsConfig: UpstreamsConfig.Upstream<*>,
        defaultOptions: Map<Chain, ChainOptions.PartialOptions>,
    ): UpstreamCreationData {
        return when (type) {
            BlockchainType.ETHEREUM -> ethereumUpstreamCreator.createUpstream(upstreamsConfig, defaultOptions)
            BlockchainType.BITCOIN -> bitcoinUpstreamCreator.createUpstream(upstreamsConfig, defaultOptions)
            else -> genericUpstreamCreator.createUpstream(upstreamsConfig, defaultOptions)
        }
    }
}
