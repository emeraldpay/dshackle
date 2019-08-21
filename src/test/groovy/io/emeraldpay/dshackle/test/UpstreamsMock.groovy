package io.emeraldpay.dshackle.test


import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.ChainUpstreams
import io.emeraldpay.dshackle.upstream.QuorumBasedMethods
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux

class UpstreamsMock implements Upstreams {

    private Map<Chain, QuorumBasedMethods> target = [:]
    private Map<Chain, AggregatedUpstream> upstreams = [:]

    UpstreamsMock(Chain chain, Upstream up) {
        addUpstream(chain, up)
    }
    UpstreamsMock(Chain chain1, Upstream up1, Chain chain2, Upstream up2) {
        addUpstream(chain1, up1)
        addUpstream(chain2, up2)
    }

    @Override
    AggregatedUpstream addUpstream(@NotNull Chain chain, @NotNull Upstream up) {
        if (!upstreams.containsKey(chain)) {
            upstreams[chain] = new ChainUpstreams(chain, [up], targetFor(chain), TestingCommons.objectMapper())
        } else {
            upstreams[chain].addUpstream(up)
        }
        return upstreams[chain]
    }

    @Override
    AggregatedUpstream getUpstream(@NotNull Chain chain) {
        return upstreams[chain]
    }

    @Override
    List<Chain> getAvailable() {
        return upstreams.keySet().toList()
    }

    @Override
    Flux<Chain> observeChains() {
        return Flux.fromIterable(getAvailable())
    }

    @Override
    QuorumBasedMethods targetFor(@NotNull Chain chain) {
        if (target[chain] == null) {
            QuorumBasedMethods targets = new QuorumBasedMethods(TestingCommons.objectMapper(), chain)
            target[chain] = targets
        }
        return target[chain]
    }

    @Override
    boolean isAvailable(@NotNull Chain chain) {
        return upstreams.containsKey(chain)
    }

}
