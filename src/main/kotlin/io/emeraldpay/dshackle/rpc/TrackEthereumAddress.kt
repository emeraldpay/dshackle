/**
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
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.BlockTag
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.toFlux
import reactor.core.scheduler.Scheduler
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.PostConstruct

@Service
class TrackEthereumAddress(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val upstreamScheduler: Scheduler
) : TrackAddress {

    private val log = LoggerFactory.getLogger(TrackEthereumAddress::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedAddress>>()
    private val seq = AtomicLong(0)

    @PostConstruct
    fun init() {
        upstreams.observeChains().subscribe { chain ->
            if (!clients.containsKey(chain)) {
                clients[chain] = ConcurrentLinkedQueue()
                upstreams.getUpstream(chain)?.getHead()?.let { head ->
                    head.getFlux().subscribe { updateBalancesAll(chain) }
                }
            }
        }
    }

    @Scheduled(fixedDelay = 120_000)
    fun pingOld() {
        val period = Duration.ofMinutes(15)
        upstreams.getAvailable().forEach { chain ->
            clients[chain]?.let { clients ->
                clients.toFlux().filter {
                    it.lastPing < Instant.now().minus(period)
                }.subscribe {
                    notify(it)
                }
            }
        }
    }

    override fun isSupported(chain: Chain): Boolean {
        return BlockchainType.fromBlockchain(chain) == BlockchainType.ETHEREUM && upstreams.isAvailable(chain)
    }

    private fun startTracking(client: TrackedAddress) {
        clients[client.chain]?.add(client) ?: log.warn("Chain ${client.chain} is not available for tracking")
    }

    private fun stopTracking(client: TrackedAddress) {
        clients[client.chain]?.removeIf {
            it.id == client.id
        } ?: log.warn("Chain ${client.chain} is not available for tracking")
    }

    fun isTracked(chain: Chain, address: Address): Boolean {
        return clients[chain]?.any { it.address == address } ?: false
    }

    private fun initializeSimple(request: BlockchainOuterClass.BalanceRequest): Flux<SimpleAddress> {
        val chain = Chain.byId(request.asset.chainValue)
        if (!upstreams.isAvailable(chain)) {
            return Flux.error(SilentException.UnsupportedBlockchain(request.asset.chainValue))
        }
        if (request.asset.code?.toLowerCase() != "ether") {
            return Flux.error(SilentException("Unsupported asset ${request.asset.code}"))
        }
        return when {
            request.address.addrTypeCase == Common.AnyAddress.AddrTypeCase.ADDRESS_SINGLE ->
                Flux.just(simpleAddress(request.address.addressSingle, chain))
            request.address.addrTypeCase == Common.AnyAddress.AddrTypeCase.ADDRESS_MULTI ->
                Flux.fromIterable(request.address.addressMulti.addressesList)
                        .map { simpleAddress(it, chain) }
            else -> {
                log.error("Unsupported address type: ${request.address.addrTypeCase}")
                Flux.empty()
            }
        }
    }

    private fun initializeSubscription(request: BlockchainOuterClass.BalanceRequest, observer: TopicProcessor<BlockchainOuterClass.AddressBalance>): Flux<TrackedAddress> {
        return initializeSimple(request)
                .map {
                    it.asTracked(observer, seq.incrementAndGet())
                }
    }

    override fun subscribe(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        val bus = TopicProcessor.create<BlockchainOuterClass.AddressBalance>()
        return initializeSubscription(request, bus)
                .flatMap { tracked ->
                    val current = getBalance(tracked).map {
                        tracked.withBalance(it)
                    }.doOnNext {
                        startTracking(it)
                    }.map {
                        buildResponse(it)
                    }
                    Flux.merge(current, bus).doFinally { stopTracking(tracked) }
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
    }

    override fun getBalance(request: BlockchainOuterClass.BalanceRequest): Flux<BlockchainOuterClass.AddressBalance> {
        return initializeSimple(request)
                .flatMap { a -> getBalance(a).map { a.withBalance(it) } }
                .map { buildResponse(it) }
    }

    private fun simpleAddress(address: Common.SingleAddress, chain: Chain): SimpleAddress {
        val addressParsed = Address.from(address.address)
        return SimpleAddress(
                chain,
                addressParsed
        )
    }

    private fun updateBalancesAll(chain: Chain) {
        clients[chain]?.let { all ->
            all.toFlux()
                    .buffer(20)
                    .map { group ->
                        updateBalances(chain, group).subscribe { updated -> notify(updated) }
                    }
                    .subscribe()
        }
    }

    fun getBalance(addr: SimpleAddress): Mono<Wei> {
        val up = upstreams.getUpstream(addr.chain) as AggregatedUpstream<EthereumApi>?
                ?: return Mono.error(SilentException.UnsupportedBlockchain(addr.chain))
        return up.getApi(Selector.empty)
                .flatMap { api -> api.executeAndConvert(Commands.eth().getBalance(addr.address, BlockTag.LATEST)) }
                .timeout(Defaults.timeout)
    }

    private fun updateBalances(chain: Chain, group: List<TrackedAddress>): Flux<TrackedAddress> {
        val up = upstreams.getUpstream(chain) ?: return Flux.empty<TrackedAddress>()
        return group.toFlux()
                .parallel(8).runOn(upstreamScheduler)
                .flatMap { a ->
                    getBalance(a).map { Update(a, it) }
                }
                .sequential()
                .filter {
                    it.addr.balance == null || it.addr.balance != it.value
                }
                .doOnNext {
                    it.addr.balance = it.value
                }
                .map {
                    it.addr
                }
    }

    private fun buildResponse(address: SimpleAddress): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
                .setBalance(address.balance!!.amount!!.toString(10))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(address.chain.id)
                        .setCode("ETHER"))
                .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.toHex()))
                .build()
    }

    private fun notify(address: TrackedAddress) {
        address.lastPing = Instant.now()
        address.stream.onNext(buildResponse(address))
    }

    class Update(val addr: TrackedAddress, val value: Wei)

    open class SimpleAddress(val chain: Chain, val address: Address, var balance: Wei? = null) {
        fun asTracked(stream: TopicProcessor<BlockchainOuterClass.AddressBalance>, id: Long): TrackedAddress {
            return TrackedAddress(chain, stream, address, balance = this.balance, id = id)
        }

        open fun withBalance(balance: Wei) = SimpleAddress(chain, address, balance)
    }

    class TrackedAddress(chain: Chain,
                         val stream: TopicProcessor<BlockchainOuterClass.AddressBalance>,
                         address: Address,
                         var lastPing: Instant = Instant.now(),
                         balance: Wei? = null,
                         val id: Long
    ): SimpleAddress(chain, address, balance) {
        override fun withBalance(balance: Wei) = TrackedAddress(chain, stream, address, lastPing, balance, id)
    }
}