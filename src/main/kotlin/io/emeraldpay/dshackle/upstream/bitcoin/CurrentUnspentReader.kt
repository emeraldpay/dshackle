package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class CurrentUnspentReader(
    upstreams: BitcoinMultistream,
    esploraClient: EsploraClient?,
) : UnspentReader {
    companion object {
        private val log = LoggerFactory.getLogger(CurrentUnspentReader::class.java)
    }

    private val delegate: UnspentReader =
        if (esploraClient != null) {
            EsploraUnspentReader(esploraClient)
        } else if (upstreams.upstreams.any { it.isGrpc() && it.getCapabilities().contains(Capability.BALANCE) }) {
            RemoteUnspentReader(upstreams)
        } else {
            RpcUnspentReader(upstreams)
        }

    override fun read(key: Address): Mono<List<SimpleUnspent>> = delegate.read(key)
}
