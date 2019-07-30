package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.AvailableChains
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockTag
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.toFlux
import reactor.math.sum
import java.lang.Exception
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import javax.annotation.PostConstruct

@Service
class TrackAddress(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val availableChains: AvailableChains
) {

    private val log = LoggerFactory.getLogger(TrackAddress::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedAddress>>()

    @PostConstruct
    fun init() {
        availableChains.observe().subscribe { chain ->
            if (!clients.containsKey(chain)) {
                clients[chain] = ConcurrentLinkedQueue()
                upstreams.getUpstream(chain)?.getHead()?.let { head ->
                    head.getFlux().subscribe { verifyAll(chain) }
                }
            }
        }
    }

    @Scheduled(fixedDelay = 120_000)
    fun pingOld() {
        val period = Duration.ofMinutes(15)
        availableChains.getAll().forEach { chain ->
            clients[chain]?.let { clients ->
                clients.toFlux().filter {
                    it.lastPing < Instant.now().minus(period)
                }.subscribe {
                    notify(it)
                }
            }
        }
    }

    fun initializeSimple(request: BlockchainOuterClass.BalanceRequest): Flux<SimpleAddress> {
        val chain = Chain.byId(request.asset.chainValue)
        if (!availableChains.supports(chain)) {
            return Flux.error(Exception("Unsupported chain ${request.asset.chainValue}"))
        }
        if (request.asset.code?.toLowerCase() != "ether") {
            return Flux.error(Exception("Unsupported asset ${request.asset.code}"))
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

    fun initializeSubscription(request: BlockchainOuterClass.BalanceRequest, observer: TopicProcessor<BlockchainOuterClass.AddressBalance>): Flux<TrackedAddress> {
        return initializeSimple(request)
                .map {
                    it.asTracked(observer)
                }
    }

    fun send(request: BlockchainOuterClass.BalanceRequest, addresses: List<TrackedAddress>): Mono<Long> {
        val chain = Chain.byId(request.asset.chainValue)
        return verify(chain, addresses)
                .map { updated -> notify(updated); 1 }
                .sum()
    }

    fun subscribe(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.flatMapMany { request ->
            val chain = Chain.byId(request.asset.chainValue)
            val sender = TopicProcessor.create<BlockchainOuterClass.AddressBalance>()
            initializeSubscription(request, sender)
                    .doOnNext { tracked -> clients[chain]?.add(tracked) }
                    .thenMany(sender)
        }
    }

    fun getBalance(requestMono: Mono<BlockchainOuterClass.BalanceRequest>): Flux<BlockchainOuterClass.AddressBalance> {
        return requestMono.flatMapMany { request ->
            initializeSimple(request)
                    .flatMap { getBalance(it) }
                    .map { process(it) }
        }
    }

    private fun simpleAddress(address: Common.SingleAddress, chain: Chain): SimpleAddress {
        val addressParsed = Address.from(address.address)
        return SimpleAddress(
                chain,
                addressParsed
        )
    }

    private fun verifyAll(chain: Chain) {
        clients[chain]?.let { all ->
            all.toFlux()
                    .buffer(20)
                    .map { group ->
                        verify(chain, group).subscribe { updated -> notify(updated) }
                    }
                    .subscribe()
        }
    }

    fun getBalance(addr: SimpleAddress): Mono<SimpleAddress> {
        val up = upstreams.getUpstream(addr.chain) ?: return Mono.error(Exception("Unsupported chain: ${addr.chain}"))
        return up.getApi()
                .executeAndConvert(Commands.eth().getBalance(addr.address, BlockTag.LATEST))
                .timeout(Duration.ofSeconds(15))
                .map { value ->
                    addr.withBalance(value)
                }
    }

    private fun verify(chain: Chain, group: List<TrackedAddress>): Flux<TrackedAddress> {
        val up = upstreams.getUpstream(chain) ?: return Flux.empty<TrackedAddress>()
        return group.toFlux()
                .flatMap { a ->
                    getBalance(a).map { Update(a, it.balance!!) }
                }
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

    private fun process(address: SimpleAddress): BlockchainOuterClass.AddressBalance {
        return BlockchainOuterClass.AddressBalance.newBuilder()
                .setBalance(address.balance!!.amount!!.toString(10))
                .setAsset(Common.Asset.newBuilder()
                        .setChainValue(address.chain.id)
                        .setCode("ETHER")
                )
                .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.toHex()))
                .build()
    }

    private fun notify(address: TrackedAddress) {
        address.stream.onNext(process(address))
        address.lastPing = Instant.now()
    }

    class Update(val addr: TrackedAddress, val value: Wei)

    open class SimpleAddress(val chain: Chain, val address: Address, var balance: Wei? = null) {
        fun asTracked(stream: TopicProcessor<BlockchainOuterClass.AddressBalance>): TrackedAddress {
            return TrackedAddress(chain, stream, address, balance = this.balance)
        }

        open fun withBalance(balance: Wei) = SimpleAddress(chain, address, balance)
    }

    class TrackedAddress(chain: Chain,
                         val stream: TopicProcessor<BlockchainOuterClass.AddressBalance>,
                         address: Address,
                         var lastPing: Instant = Instant.now(),
                         balance: Wei? = null
    ): SimpleAddress(chain, address, balance) {
        override fun withBalance(balance: Wei) = TrackedAddress(chain, stream, address, lastPing, balance);
    }
}