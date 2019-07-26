package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain

interface Upstreams {
    fun getOrCreateUpstream(chain: Chain): AggregatedUpstreams
    fun getUpstream(chain: Chain): AggregatedUpstreams?
    fun getAvailable(): List<Chain>
}