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

import io.emeraldpay.api.BlockchainType
import io.emeraldpay.api.Chain
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.MultistreamHolder
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinMultistream
import io.emeraldpay.dshackle.upstream.bitcoin.data.SimpleUnspent
import io.emeraldpay.dshackle.upstream.grpc.BitcoinGrpcUpstream
import org.apache.commons.lang3.StringUtils
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.params.TestNet3Params
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct

@Service
class TrackBitcoinAddress(
    @Autowired private val multistreamHolder: MultistreamHolder
) : TrackAddress {

    companion object {
        private val log = LoggerFactory.getLogger(TrackBitcoinAddress::class.java)
    }

    override fun isSupported(chain: Chain, asset: String): Boolean {
        return (asset == "bitcoin" || asset == "btc" || asset == "satoshi") &&
            BlockchainType.from(chain) == BlockchainType.BITCOIN && multistreamHolder.isAvailable(chain)
    }

    /**
     * Keep tracking of the current state of local upstreams. True for a chain that has an upstream with balance data.
     */
    private val localBalanceAvailable: MutableMap<Chain, Boolean> = ConcurrentHashMap()

    /**
     * Criteria for a remote grpc upstream that can provide a balance
     */
    private val balanceUpstreamMatcher = Selector.LocalAndMatcher(
        Selector.GrpcMatcher(),
        Selector.CapabilityMatcher(Capability.BALANCE)
    )

    @PostConstruct
    fun listenChains() {
        multistreamHolder.observeChains().subscribe { chain ->
            multistreamHolder.getUpstream(chain)?.let { mup ->
                val available = mup.getAll().any { up ->
                    !up.isGrpc() && up.getCapabilities().contains(Capability.BALANCE)
                }
                setBalanceAvailability(chain, available)
            }
        }
    }

    fun setBalanceAvailability(chain: Chain, enabled: Boolean) {
        localBalanceAvailable[chain] = enabled
    }

    /**
     * @return true if the current instance has data sources to provide the balance
     */
    fun isBalanceAvailable(chain: Chain): Boolean {
        return localBalanceAvailable[chain] ?: false
    }

    fun allAddresses(api: BitcoinMultistream, request: BlockchainOuterClass.BalanceRequest): Flux<String> {
        if (!request.hasAddress()) {
            return Flux.empty()
        }
        return when {
            request.address.hasAddressXpub() -> {
                val xpubAddresses = api.getXpubAddresses()
                    ?: return Flux.error(IllegalStateException("Xpub verification is not available"))

                val addressXpub = request.address.addressXpub
                if (StringUtils.isEmpty(addressXpub.xpub)) {
                    return Flux.error(IllegalArgumentException("xpub string is empty"))
                }
                val xpub = addressXpub.xpub
                val start = Math.max(0, addressXpub.start).toInt()
                val limit = Math.min(100, Math.max(1, addressXpub.limit)).toInt()
                xpubAddresses.activeAddresses(xpub, start, limit)
                    .map { it.toString() }
                    .doOnError { t -> log.error("Failed to process xpub. ${t.javaClass}:${t.message}") }
            }
            request.address.hasAddressSingle() -> {
                Flux.just(request.address.addressSingle.address)
            }
            request.address.hasAddressMulti() -> {
                Flux.fromIterable(
                    request.address.addressMulti.addressesList
                        .map { addr -> addr.address }
                        // TODO why sorted?
                        .sorted()
                )
            }
            else -> Flux.error(IllegalArgumentException("Unsupported address type"))
        }
    }

    fun requestBalances(
        chain: Chain,
        api: BitcoinMultistream,
        addresses: Flux<String>,
        includeUtxo: Boolean
    ): Flux<AddressBalance> {
        return addresses
            .map { Address(chain, it) }
            .flatMap { address ->
                balanceForAddress(api, address, includeUtxo)
            }
    }

    fun balanceForAddress(api: BitcoinMultistream, address: Address, includeUtxo: Boolean): Mono<AddressBalance> {
        return api.unspentReader
            .read(address.bitcoinAddress)
            .map { unspent ->
                totalUnspent(address, includeUtxo, unspent)
            }
            .switchIfEmpty(
                Mono.just(0).map {
                    AddressBalance(address, BigInteger.ZERO)
                }
            )
            .onErrorResume { t ->
                log.error("Failed to get unspent", t)
                Mono.empty()
            }
    }

    fun totalUnspent(address: Address, includeUtxo: Boolean, unspent: List<SimpleUnspent>): AddressBalance {
        return if (unspent.isEmpty()) {
            AddressBalance(address, BigInteger.ZERO)
        } else {
            unspent.map {
                AddressBalance(
                    address,
                    BigInteger.valueOf(it.value),
                    if (includeUtxo) listOf(BalanceUtxo(it.txid, it.vout, it.value))
                    else emptyList()
                )
            }.reduce { a, b -> a.plus(b) }
        }
    }

    fun getBalanceGrpc(api: BitcoinMultistream): Mono<ReactorBlockchainGrpc.ReactorBlockchainStub> {
        val ups = api.getApiSource(balanceUpstreamMatcher)
        ups.request(1)
        return Mono.from(ups)
            .map { up ->
                up.cast(BitcoinGrpcUpstream::class.java).remote
            }
            .timeout(Defaults.timeoutInternal, Mono.empty())
            .switchIfEmpty(
                Mono.fromCallable {
                    log.warn("No upstream providing balance for ${api.chain}")
                }
                    .then(Mono.error(SilentException.DataUnavailable("BALANCE")))
            )
    }

    fun getRemoteBalance(
        api: BitcoinMultistream,
        request: BlockchainOuterClass.BalanceRequest
    ): Flux<BlockchainOuterClass.AddressBalance> {
        return getBalanceGrpc(api).flatMapMany { remote ->
            remote.getBalance(request)
        }
    }

    fun subscribeRemoteBalance(
        api: BitcoinMultistream,
        request: BlockchainOuterClass.BalanceRequest
    ): Flux<BlockchainOuterClass.AddressBalance> {
        return getBalanceGrpc(api).flatMapMany { remote ->
            remote.subscribeBalance(request)
        }
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val upstream = multistreamHolder.getUpstream(chain)?.cast(BitcoinMultistream::class.java)
            ?: return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        return if (isBalanceAvailable(chain)) {
            val addresses = allAddresses(upstream, request)
            requestBalances(chain, upstream, addresses, request.includeUtxo)
                .map(this@TrackBitcoinAddress::buildResponse)
                .doOnError { t ->
                    log.error("Failed to get balance", t)
                }
        } else {
            getRemoteBalance(upstream, request)
                .doOnError { t ->
                    log.error("Failed to get balance from remote", t)
                }
        }
    }

    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val chain = Chain.byId(request.asset.chainValue)
        val upstream = multistreamHolder.getUpstream(chain)?.cast(BitcoinMultistream::class.java)
            ?: return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        if (isBalanceAvailable(chain)) {
            val addresses = allAddresses(upstream, request).cache()
            val following = upstream.getHead().getFlux()
                .flatMap {
                    requestBalances(chain, upstream, Flux.from(addresses), request.includeUtxo)
                }
            val last = HashMap<String, BigInteger>()
            val result = following
                .filter { curr ->
                    val prev = last[curr.address.address]
                    // TODO utxo can change without changing balance
                    val changed = prev == null || curr.balance != prev
                    if (changed) {
                        last[curr.address.address] = curr.balance
                    }
                    changed
                }

            return result.map(this@TrackBitcoinAddress::buildResponse)
        } else {
            return subscribeRemoteBalance(upstream, request)
        }
    }

    private fun buildResponse(address: AddressBalance): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
            .setBalance(address.balance.toString(10))
            .setAsset(
                Common.Asset.newBuilder()
                    .setChainValue(address.address.chain.id)
                    .setCode("BTC")
            )
            .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.address))
            .addAllUtxo(
                address.utxo.map { utxo ->
                    BlockchainOuterClass.Utxo.newBuilder()
                        .setBalance(utxo.value.toString())
                        .setIndex(utxo.vout.toLong())
                        .setTxId(utxo.txid)
                        .build()
                }
            )
            .build()
    }

    open class AddressBalance(
        val address: Address,
        var balance: BigInteger = BigInteger.ZERO,
        var utxo: List<BalanceUtxo> = emptyList()
    ) {
        constructor(chain: Chain, address: String, balance: BigInteger) : this(Address(chain, address), balance)

        fun plus(other: AddressBalance) = AddressBalance(address, balance + other.balance, utxo.plus(other.utxo))
    }

    open class BalanceUtxo(val txid: String, val vout: Int, val value: Long)

    // TODO use bitcoin class for address
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
