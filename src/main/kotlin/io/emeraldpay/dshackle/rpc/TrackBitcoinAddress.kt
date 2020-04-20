package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinApi
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.util.function.Tuples
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*

@Service
class TrackBitcoinAddress(
        @Autowired private val upstreams: Upstreams
) : TrackAddress {

    companion object {
        private val log = LoggerFactory.getLogger(TrackBitcoinAddress::class.java)
    }

    override fun isSupported(chain: Chain): Boolean {
        return BlockchainType.fromBlockchain(chain) == BlockchainType.BITCOIN && upstreams.isAvailable(chain)
    }

    override fun getBalance(req: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        if (!req.hasAddress()) {
            return Flux.error(SilentException("Address not provided"))
        }
        val chain = Chain.byId(req.asset.chainValue)
        val upstream = upstreams.getUpstream(chain)?.castApi(BitcoinApi::class.java)
                ?: return Flux.error(SilentException.UnsupportedBlockchain(req.asset.chainValue))
        val addressesAll = when {
            req.address.hasAddressSingle() -> {
                listOf(req.address.addressSingle.address)
            }
            req.address.hasAddressMulti() -> {
                req.address.addressMulti.addressesList
                        .map { addr -> addr.address }
            }
            else -> {
                return Flux.error(SilentException("Unsupported address"))
            }
        }
        val result = upstream.getApi(Selector.empty).flatMapMany { api ->
            val addresses = addressesAll.sorted()
            val results = api.executeAndResult(0, "listunspent", emptyList(), List::class.java)
                    .flatMapMany { unspents ->
                        val result = getTotal(chain, addresses, unspents)
                        Flux.fromIterable(result)
                    }
            results.map { addr ->
                buildResponse(addr)
            }
        }
        return result
    }

    fun getTotal(chain: Chain, addresses: List<String>, unspents: List<*>): List<AddressBalance> {
        return unspents.asSequence()
                .filterIsInstance<Map<String, Any>>()
                .filter { unspent ->
                    unspent.containsKey("address")
                            && Collections.binarySearch(addresses, unspent["address"] as String) >= 0
                            && unspent.containsKey("amount")
                }
                .map {
                    Tuples.of(it["address"] as String, it["amount"] as Number)
                }
                .map {
                    AddressBalance(chain, it.t1,
                            //use toString because toDecimal makes rounding
                            BigDecimal(it.t2.toString()).multiply(BigDecimal.TEN.pow(8)).toBigInteger()
                    )
                }
                .plus(
                        //add default ZERO value
                        addresses.map {
                            AddressBalance(chain, it, BigInteger.ZERO)
                        }
                )
                .groupBy {
                    it.address
                }
                .map {
                    it.value.reduceRight { x, acc ->
                        x.plus(acc)
                    }
                }.toList()
    }


    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        return Flux.error(SilentException("Not Implemented"))
    }

    private fun buildResponse(address: AddressBalance): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
                .setBalance(address.balance.toString(10))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(address.chain.id)
                        .setCode("BTC"))
                .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address))
                .build()
    }

    open class AddressBalance(val chain: Chain, val address: String, var balance: BigInteger = BigInteger.ZERO) {
        open fun withBalance(balance: BigInteger) = AddressBalance(chain, address, balance)

        open fun plus(other: AddressBalance) = AddressBalance(chain, address, balance + other.balance)

    }
}