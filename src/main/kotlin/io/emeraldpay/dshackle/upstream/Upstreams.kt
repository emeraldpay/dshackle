package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain
import reactor.core.publisher.Flux

interface Upstreams {
    fun addUpstream(chain: Chain, up: Upstream): AggregatedUpstream
    fun getUpstream(chain: Chain): AggregatedUpstream?
    fun getAvailable(): List<Chain>
    fun observeChains(): Flux<Chain>
    fun targetFor(chain: Chain): CallMethods
    fun isAvailable(chain: Chain): Boolean
}