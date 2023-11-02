package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.DefaultUpstream

interface ConnectorFactory {
    fun create(
        upstream: DefaultUpstream,
        chain: Chain,
    ): GenericConnector

    fun isValid(): Boolean
}
