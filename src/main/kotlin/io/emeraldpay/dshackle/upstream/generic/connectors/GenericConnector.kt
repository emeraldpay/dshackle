package io.emeraldpay.dshackle.upstream.generic.connectors

import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import reactor.core.publisher.Flux

interface GenericConnector : Lifecycle {
    fun getHead(): Head

    fun headLivenessEvents(): Flux<HeadLivenessState>

    fun getIngressReader(): ChainReader

    fun getIngressSubscription(): IngressSubscription
}
