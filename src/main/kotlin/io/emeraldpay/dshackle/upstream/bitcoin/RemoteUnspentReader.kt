package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.api.proto.BlockchainOuterClass.BalanceRequest
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.grpc.BitcoinGrpcUpstream
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class RemoteUnspentReader(
    val upstreams: BitcoinMultistream
) : UnspentReader {

    companion object {
        private val log = LoggerFactory.getLogger(RemoteUnspentReader::class.java)
    }

    private val selector = Selector.LocalAndMatcher(
        Selector.GrpcMatcher(),
        Selector.CapabilityMatcher(Capability.BALANCE)
    )

    override fun read(key: Address): Mono<List<SimpleUnspent>> {
        val apis = upstreams.getApiSource(selector)
        apis.request(1)
        return Mono.from(apis)
            .map { up ->
                up.cast(BitcoinGrpcUpstream::class.java).remote
            }
            .flatMapMany {
                val request = BalanceRequest.newBuilder()
                    .build()
                it.getBalance(request)
            }
            .map { resp ->
                resp.utxoList.map { utxo ->
                    SimpleUnspent(
                        utxo.txId,
                        utxo.index.toInt(),
                        utxo.balance.toLong(),
                    )
                }
            }
            .reduce(List<SimpleUnspent>::plus)
    }
}
