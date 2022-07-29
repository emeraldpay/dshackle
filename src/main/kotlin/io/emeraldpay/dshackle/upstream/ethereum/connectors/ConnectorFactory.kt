package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.grpc.Chain

interface ConnectorFactory {
    fun create(upstream: DefaultUpstream, validator: EthereumUpstreamValidator, chain: Chain): EthereumConnector
    fun isValid(): Boolean
}
