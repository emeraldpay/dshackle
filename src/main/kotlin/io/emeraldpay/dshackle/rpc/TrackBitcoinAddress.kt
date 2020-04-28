/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.bitcoin.DirectBitcoinApi
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.util.function.Tuples
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import kotlin.collections.HashMap

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

    fun allAddresses(request: BlockchainOuterClass.BalanceRequest): List<String>? {
        if (!request.hasAddress()) {
            return null
        }
        return when {
            request.address.hasAddressSingle() -> {
                listOf(request.address.addressSingle.address)
            }
            request.address.hasAddressMulti() -> {
                request.address.addressMulti.addressesList
                        .map { addr -> addr.address }
                        .sorted()
            }
            else -> null
        }
    }

    fun requestBalances(chain: Chain, api: DirectBitcoinApi, addresses: List<String>): Flux<AddressBalance> {
        return api.executeAndResult(0, "listunspent", emptyList(), List::class.java)
                .flatMapMany { unspents ->
                    val result = getTotal(chain, addresses, unspents)
                    Flux.fromIterable(result)
                }
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val upstream = upstreams.getUpstream(chain)?.castApi(DirectBitcoinApi::class.java)
                ?: return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        val addresses = allAddresses(request) ?: return Flux.error(SilentException("Unsupported address"))
        if (addresses.isEmpty()) {
            return Flux.empty()
        }
        val result = upstream.getApi(Selector.empty).flatMapMany { api ->
            requestBalances(chain, api, addresses)
                    .map(this@TrackBitcoinAddress::buildResponse)
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
        val chain = Chain.byId(request.asset.chainValue)
        val upstream = upstreams.getUpstream(chain)?.castApi(DirectBitcoinApi::class.java)
                ?: return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        val addresses = allAddresses(request) ?: return Flux.error(SilentException("Unsupported address"))
        if (addresses.isEmpty()) {
            return Flux.empty()
        }
        val initial = upstream.getApi(Selector.empty).flatMapMany { api ->
            requestBalances(chain, api, addresses)
        }
        val following = upstream.getHead().getFlux()
                .flatMap { block ->
                    upstream.getApi(Selector.empty).flatMapMany { api ->
                        requestBalances(chain, api, addresses)
                    }
                }
        val last = HashMap<Address, BigInteger>()
        val result = Flux.merge(initial, following)
                .filter { curr ->
                    val prev = last[curr.address]
                    val updated = prev == null || curr.balance != prev
                    last[curr.address] = curr.balance
                    updated
                }

        return result.map(this@TrackBitcoinAddress::buildResponse)
    }

    private fun buildResponse(address: AddressBalance): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
                .setBalance(address.balance.toString(10))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(address.address.chain.id)
                        .setCode("BTC"))
                .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.address))
                .build()
    }

    open class AddressBalance(val address: Address, var balance: BigInteger = BigInteger.ZERO) {
        constructor(chain: Chain, address: String, balance: BigInteger) : this(Address(chain, address), balance)

        fun plus(other: AddressBalance) = AddressBalance(address, balance + other.balance)
    }

    data class Address(val chain: Chain, val address: String)
}