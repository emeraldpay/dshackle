package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator

interface ConnectorFactory {
    fun create(
        upstream: DefaultUpstream,
        validator: EthereumUpstreamValidator,
        chain: Chain,
        skipEnhance: Boolean
    ): EthereumConnector

    fun isValid(): Boolean
}
