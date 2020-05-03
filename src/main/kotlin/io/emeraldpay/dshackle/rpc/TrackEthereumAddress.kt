/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
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
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockTag
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class TrackEthereumAddress(
        @Autowired private val upstreams: Upstreams
) : TrackAddress {

    private val log = LoggerFactory.getLogger(TrackEthereumAddress::class.java)

    override fun isSupported(chain: Chain): Boolean {
        return BlockchainType.fromBlockchain(chain) == BlockchainType.ETHEREUM && upstreams.isAvailable(chain)
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        return initAddress(request)
                .flatMap { a -> getBalance(a).map { a.withBalance(it) } }
                .map { buildResponse(it) }
    }

    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val head = upstreams.getUpstream(chain)?.getHead()?.getFlux() ?: Flux.empty()
        val balances = initAddress(request)
                .flatMap { tracked ->
                    val current = getBalance(tracked)
                            .map {
                                tracked.withBalance(it)
                            }
                    val updates = head
                            .flatMap {
                                getBalance(tracked)
                            }.map {
                                tracked.withBalance(it)
                            }

                    Flux.concat(current, updates)
                            .distinctUntilChanged {
                                it.balance ?: Wei.ZERO
                            }
                }
                .doOnError { t ->
                    if (t is SilentException) {
                        if (t is SilentException.UnsupportedBlockchain) {
                            log.warn("Unsupported blockchain: ${t.blockchainId}")
                        }
                        log.debug("Failed to process subscription", t)
                    } else {
                        log.warn("Failed to process subscription", t)
                    }
                }

        return balances.map {
            buildResponse(it)
        }
    }

    private fun initAddress(request: BlockchainOuterClass.BalanceRequest): Flux<TrackedAddress> {
        val chain = Chain.byId(request.asset.chainValue)
        if (!upstreams.isAvailable(chain)) {
            return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        }
        if (request.asset.code?.toLowerCase() != "ether") {
            return Flux.error(SilentException("Unsupported asset ${request.asset.code}"))
        }
        return when (request.address.addrTypeCase) {
            Common.AnyAddress.AddrTypeCase.ADDRESS_SINGLE ->
                Flux.just(createAddress(request.address.addressSingle, chain))
            Common.AnyAddress.AddrTypeCase.ADDRESS_MULTI ->
                Flux.fromIterable(request.address.addressMulti.addressesList)
                        .map { createAddress(it, chain) }
            else -> {
                log.error("Unsupported address type: ${request.address.addrTypeCase}")
                Flux.empty()
            }
        }
    }

    private fun createAddress(address: Common.SingleAddress, chain: Chain): TrackedAddress {
        val addressParsed = Address.from(address.address)
        return TrackedAddress(
                chain,
                addressParsed
        )
    }

    fun getBalance(addr: TrackedAddress): Mono<Wei> {
        val up = upstreams.getUpstream(addr.chain) as AggregatedUpstream<EthereumApi>?
                ?: return Mono.error(SilentException.UnsupportedBlockchain(addr.chain))
        return up.getApi(Selector.empty)
                .flatMap { api -> api.executeAndConvert(Commands.eth().getBalance(addr.address, BlockTag.LATEST)) }
                .timeout(Defaults.timeout)
    }

    private fun buildResponse(address: TrackedAddress): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
                .setBalance(address.balance!!.amount!!.toString(10))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(address.chain.id)
                        .setCode("ETHER"))
                .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.toHex()))
                .build()
    }

    class TrackedAddress(val chain: Chain,
                         val address: Address,
                         val balance: Wei? = null
    ) {
        fun withBalance(balance: Wei) = TrackedAddress(chain, address, balance)
    }
}