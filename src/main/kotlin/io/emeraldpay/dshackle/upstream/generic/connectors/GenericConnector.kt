package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.Lifecycle
import reactor.core.publisher.Flux

interface GenericConnector : Lifecycle {
    fun getHead(): Head

    fun hasLiveSubscriptionHead(): Flux<Boolean>

    fun getIngressReader(): JsonRpcReader

    fun getIngressSubscription(): IngressSubscription
}
