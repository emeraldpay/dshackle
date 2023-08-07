package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription

interface EthereumConnector : Lifecycle {
    fun getHead(): Head

    fun getConnectorMode(): EthereumConnectorFactory.ConnectorMode

    fun getIngressReader(): JsonRpcReader

    fun getIngressSubscription(): EthereumIngressSubscription
}
