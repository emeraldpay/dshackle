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
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.grpc.Chain
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.params.TestNet3Params
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger
import kotlin.collections.HashMap

@Service
class TrackBitcoinAddress(
        @Autowired private val multistreamHolder: MultistreamHolder
) : TrackAddress {

    companion object {
        private val log = LoggerFactory.getLogger(TrackBitcoinAddress::class.java)
    }

    override fun isSupported(chain: Chain, asset: String): Boolean {
        return (asset == "bitcoin" || asset == "btc" || asset == "satoshi")
                && BlockchainType.fromBlockchain(chain) == BlockchainType.BITCOIN && multistreamHolder.isAvailable(chain)
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

    fun requestBalances(chain: Chain, api: BitcoinMultistream, addresses: List<String>): Flux<AddressBalance> {
        return Flux.fromIterable(addresses)
                .map { Address(chain, it) }
                .flatMap { address ->
                    api.getReader()
                            .listUnspent(address.bitcoinAddress)
                            .map { unspents ->
                                getTotal(address, unspents)
                            }
                            .onErrorResume { t ->
                                log.error("Failed to get unspent", t)
                                Mono.empty()
                            }
                }
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val upstream = multistreamHolder.getUpstream(chain)?.cast(BitcoinMultistream::class.java)
                ?: return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        val addresses = allAddresses(request) ?: return Flux.error(SilentException("Unsupported address"))
        if (addresses.isEmpty()) {
            return Flux.empty()
        }
        return requestBalances(chain, upstream, addresses)
                .map(this@TrackBitcoinAddress::buildResponse)
    }

    fun getTotal(address: Address, unspents: List<SimpleUnspent>): AddressBalance {
        val total = if (unspents.isEmpty()) {
            0L
        } else {
            unspents.map { it.value }.reduce(Long::plus)
        }
        return AddressBalance(address, BigInteger.valueOf(total))
    }


    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val upstream = multistreamHolder.getUpstream(chain)?.cast(BitcoinMultistream::class.java)
                ?: return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        val addresses = allAddresses(request) ?: return Flux.error(SilentException("Unsupported address"))
        if (addresses.isEmpty()) {
            return Flux.empty()
        }
        val initial = requestBalances(chain, upstream, addresses)
        val following = upstream.getHead().getFlux()
                .flatMap { block ->
                    requestBalances(chain, upstream, addresses)
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

    class Address(val chain: Chain, val address: String) {
        val network = if (chain == Chain.BITCOIN) {
            MainNetParams()
        } else {
            TestNet3Params()
        }
        val bitcoinAddress = org.bitcoinj.core.Address.fromString(
                network, address
        )
    }
}