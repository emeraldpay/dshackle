package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.api.proto.BlockchainOuterClass.BalanceRequest
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.Common.AnyAddress
import io.emeraldpay.api.proto.Common.SingleAddress
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.grpc.BitcoinGrpcUpstream
import org.bitcoinj.core.Address
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class RemoteUnspentReader(
    val upstreams: BitcoinMultistream,
) : UnspentReader {
    companion object {
        private val log = LoggerFactory.getLogger(RemoteUnspentReader::class.java)
    }

    private val selector =
        Selector.LocalAndMatcher(
            Selector.GrpcMatcher(),
            Selector.CapabilityMatcher(Capability.BALANCE),
        )

    override fun read(key: Address): Mono<List<SimpleUnspent>> {
        val apis = upstreams.getApiSource(selector)
        apis.request(1)
        return Mono
            .from(apis)
            .map { up -> up.cast(BitcoinGrpcUpstream::class.java) }
            .flatMap { readFromUpstream(it, key) }
    }

    fun readFromUpstream(
        upstream: BitcoinGrpcUpstream,
        address: Address,
    ): Mono<List<SimpleUnspent>> {
        val request = createRequest(address)
        return upstream.remote
            .getBalance(request)
            .switchIfEmpty(Mono.error(SilentException.DataUnavailable("Balance not provider")))
            .doOnError { t -> log.warn("Failed to get balance from remote", t) }
            .map { resp ->
                resp.utxoList.map { utxo ->
                    SimpleUnspent(
                        utxo.txId,
                        utxo.index.toInt(),
                        utxo.balance.toLong(),
                    )
                }
            }.reduce(List<SimpleUnspent>::plus)
    }

    fun createRequest(address: Address): BalanceRequest =
        BalanceRequest
            .newBuilder()
            .setAddress(AnyAddress.newBuilder().setAddressSingle(SingleAddress.newBuilder().setAddress(address.toString())))
            .setAsset(Common.Asset.newBuilder().setChainValue(upstreams.chain.id))
            .setIncludeUtxo(true)
            .build()
}
