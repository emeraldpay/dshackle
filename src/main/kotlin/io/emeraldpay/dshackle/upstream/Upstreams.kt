package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux

interface Upstreams {
    fun getOrCreateUpstream(chain: Chain): AggregatedUpstreams
    fun getUpstream(chain: Chain): AggregatedUpstreams?
    fun getAvailable(): List<Chain>
}