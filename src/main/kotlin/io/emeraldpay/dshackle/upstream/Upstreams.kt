package io.emeraldpay.dshackle.upstream

import io.emeraldpay.grpc.Chain

interface Upstreams {
    fun ethereumUpstream(chain: Chain): AggregatedUpstreams
    fun getAvailable(): List<Chain>
}