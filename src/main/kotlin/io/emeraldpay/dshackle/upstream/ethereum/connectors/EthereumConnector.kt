package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.ethereum.EthereumIngressSubscription
import reactor.core.publisher.Flux

interface EthereumConnector : Lifecycle {
    fun getHead(): Head

    fun hasLiveSubscriptionHead(): Flux<Boolean>

    fun getIngressReader(): JsonRpcReader

    fun getIngressSubscription(): EthereumIngressSubscription
}
