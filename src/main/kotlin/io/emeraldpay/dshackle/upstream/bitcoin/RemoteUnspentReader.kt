package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import org.bitcoinj.core.Address
import reactor.core.publisher.Mono

class RemoteUnspentReader(
    val upstreams: BitcoinMultistream,
) : UnspentReader {

    private val selector = Selector.MultiMatcher(
        listOf(
            Selector.GrpcMatcher(),
            Selector.CapabilityMatcher(Capability.BALANCE),
        ),
    )

    override fun read(key: Address): Mono<List<SimpleUnspent>> {
        val apis = upstreams.getApiSource(selector)
        apis.request(1)
        return Mono.empty()
    }
}
