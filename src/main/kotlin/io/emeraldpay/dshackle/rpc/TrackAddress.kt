package io.emeraldpay.dshackle.rpc

import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.domain.Address
import io.infinitape.etherjar.domain.Wei
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockTag
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.math.sum
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.lang.Exception
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Future
import javax.annotation.PostConstruct

@Service
class TrackAddress(
        @Autowired private val upstreams: Upstreams
) {

    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedAddress>>()

    private val allChains = listOf(Chain.TESTNET_MORDEN, Chain.ETHEREUM_CLASSIC, Chain.ETHEREUM, Chain.TESTNET_KOVAN)

    @PostConstruct
    fun init() {
        allChains.forEach { chain ->
            clients[chain] = ConcurrentLinkedQueue()
            upstreams.ethereumUpstream(chain)?.head?.let { head ->
                head.getFlux().subscribe { verifyAll(chain) }
            }
        }
    }

    @Scheduled(fixedDelay = 120_000)
    fun pingOld() {
        val period = Duration.ofMinutes(15)
        allChains.forEach { chain ->
            clients[chain]?.let { clients ->
                clients.toFlux().filter {
                    it.lastPing < Instant.now().minus(period)
                }.subscribe {
                    notify(it)
                }
            }
        }
    }

    fun initializeFor(request: BlockchainOuterClass.BalanceRequest, responseObserver: StreamObserver<BlockchainOuterClass.AddressBalance>): List<TrackedAddress> {
        val chain = Chain.byId(request.asset.chainValue)
        if (!allChains.contains(chain)) {
            responseObserver.onError(Exception("Unsupported chain ${request.asset.chainValue}"))
            return Collections.emptyList()
        }
        if (request.asset.code?.toLowerCase() != "ether") {
            responseObserver.onError(Exception("Unsupported asset ${request.asset.code}"))
            return Collections.emptyList()
        }
        val new = java.util.ArrayList<TrackedAddress>()
        val observer = StreamSender<BlockchainOuterClass.AddressBalance>(responseObserver)
        if (request.address.addrTypeCase == Common.AnyAddress.AddrTypeCase.ADDRESS_SINGLE) {
            new.add(forAddress(request.address.addressSingle, chain, observer))
        } else if (request.address.addrTypeCase == Common.AnyAddress.AddrTypeCase.ADDRESS_MULTI) {
            request.address.addressMulti.addressesList.forEach { address ->
                new.add(forAddress(address, chain, observer))
            }
        }
        return new
    }

    fun send(request: BlockchainOuterClass.BalanceRequest, addresses: List<TrackedAddress>): Mono<Long> {
        val chain = Chain.byId(request.asset.chainValue)
        return verify(chain, addresses)
                .map { updated -> notify(updated); 1 }
                .sum()
    }

    fun add(request: BlockchainOuterClass.BalanceRequest, responseObserver: StreamObserver<BlockchainOuterClass.AddressBalance>) {
        val chain = Chain.byId(request.asset.chainValue)
        val new = initializeFor(request, responseObserver)
        send(request, new)
                .doFinally {
                    clients[chain]?.addAll(new)
                }
                .subscribe()
    }

    private fun forAddress(address: Common.SingleAddress, chain: Chain, observer: StreamSender<BlockchainOuterClass.AddressBalance>): TrackedAddress {
        val addressParsed = Address.from(address.address)
        return TrackedAddress(
                chain,
                observer,
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

    private fun verify(chain: Chain, group: List<TrackedAddress>): Flux<TrackedAddress> {
        val up = upstreams.ethereumUpstream(chain)!!
        return group.toFlux()
                .reduce<Tuple2<Batch, ArrayList<Update>>>(Tuples.of(Batch(), ArrayList())) { batch, a ->
                    val f = batch.t1.add(Commands.eth().getBalance(a.address, BlockTag.LATEST));
                    batch.t2.add(Update(a, f))
                    batch
                }
                .flatMap {
                    Mono.fromCompletionStage(up.api.execute(it.t1))
                            .thenReturn(it.t2)
                }
                .flatMapMany {
                    it.toFlux()
                }
                .filter {
                    it.addr.balance == null || it.addr.balance != it.value.get()
                }
                .doOnNext {
                    it.addr.balance = it.value.get()
                }
                .map {
                    it.addr
                }
    }

    private fun notify(address: TrackedAddress): Boolean {
        val sent = address.stream.send(
                BlockchainOuterClass.AddressBalance.newBuilder()
                        .setBalance(address.balance!!.amount!!.toString(10))
                        .setAsset(Common.Asset.newBuilder()
                                .setChainValue(address.chain.id)
                                .setCode("ETHER")
                        )
                        .setAddress(Common.SingleAddress.newBuilder().setAddress(address.address.toHex()))
                        .build()
        )
        if (!sent) {
            clients[address.chain]?.remove(address)
        }
        address.lastPing = Instant.now()
        return sent
    }

    class Update(val addr: TrackedAddress, val value: Future<Wei>)

    class TrackedAddress(val chain: Chain,
                         val stream: StreamSender<BlockchainOuterClass.AddressBalance>,
                         val address: Address,
                         val since: Instant = Instant.now(),
                         var lastPing: Instant = Instant.now(),
                         var balance: Wei? = null
    )
}