package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.AvailableChains
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.TopicProcessor
import reactor.core.publisher.toFlux
import reactor.core.scheduler.Scheduler
import reactor.util.function.Tuples
import java.lang.Exception
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.time.Period
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.PostConstruct
import kotlin.collections.HashMap
import kotlin.math.max
import kotlin.math.min

@Service
class TrackTx(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val availableChains: AvailableChains,
        @Autowired private val upstreamScheduler: Scheduler
) {

    companion object {
        private val ZERO_BLOCK = BlockHash.from("0x0000000000000000000000000000000000000000000000000000000000000000")
        private val FRESH_TX = Duration.ofSeconds(60)
        private val TRACK_TTL = Duration.ofHours(1)
        private val NOT_FOUND_TRACK_TTL = Duration.ofMinutes(1)
        private val NOT_MINED_TRACK_TTL = NOT_FOUND_TRACK_TTL.multipliedBy(2)
        private val PING_PERIOD = Duration.ofMinutes(5)
    }

    private val log = LoggerFactory.getLogger(TrackTx::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedTx>>()
    private val seq = AtomicLong(0)

    val notFound = ConcurrentLinkedQueue<TrackedTx>()

    @PostConstruct
    fun init() {
        availableChains.observe().subscribe { chain ->
            clients[chain] = ConcurrentLinkedQueue()
            upstreams.getUpstream(chain)?.getHead()?.let { head ->
                head.getFlux().subscribe { verifyAll(chain) }
            }
        }
    }

    @Scheduled(fixedDelay = 5000)
    fun recheckFreshNotFound() {
        val recent = Instant.now().minus(FRESH_TX)
        verifyAll(
                notFound.filter {
                    val tx = it.tx
                    !tx.status.found && tx.since.isAfter(recent)
                }.map { it.tx }
        )
    }

    @Scheduled(fixedRate = 60000)
    fun recheckMatureNotFound() {
        val recent = Instant.now().minus(FRESH_TX)
        verifyAll(
                notFound.filter {
                    !it.tx.status.found && it.tx.since.isBefore(recent)
                }.map { it.tx }
        )
    }

    fun recheckNotFound() {
        verifyAll(notFound.map { it.tx })
    }

    @Scheduled(fixedRate = 60000, initialDelay = 10000)
    fun cleanupNotFound() {
        notFound.removeIf {
            it.tx.shouldClose() || it.tx.status.found
        }
    }

    private fun trackedForChain(chain: Chain): List<TxDetails>? {
        return clients[chain]?.map { it.tx }
    }

    fun onFirstUpdate(tx: TxDetails) {
        tx.backref?.let { backref ->
            clients[tx.chain]?.add(backref)
            if (!tx.status.found) {
                notFound.add(backref)
            }
        }
    }

    fun onFinished(chain: Chain, txid: Long) {
        clients[chain]?.removeIf { x -> x.tx.id == txid }
        notFound.removeIf { x -> x.tx.id == txid }
    }

    fun onSend(tx: TxDetails) {
        val curr = tx.justNotified().makeCurrent()
        if (curr.shouldClose()) {
            curr.bus.onComplete()
        }
    }

    fun prepareTracking(request: BlockchainOuterClass.TxStatusRequest): TxDetails {
        val chain = Chain.byId(request.chainValue)
        if (!clients.containsKey(chain)) {
            throw Exception("Unsupported blockchain: ${chain}")
        }
        val bus = TopicProcessor.create<Notification>()
        val details = TxDetails(
                chain,
                bus,
                Instant.now(),
                TransactionId.from(request.txId),
                min(max(1, request.confirmationLimit), 100),
                seq.incrementAndGet()
        )
        val tracked = TrackedTx(details)
        return details.withBackref(tracked)
    }

    fun streamAllUpdates(tx: TxDetails): Flux<BlockchainOuterClass.TxStatus> {
        val current = checkForUpdate(tx)
                .doOnNext (this::onFirstUpdate)
                .map { Notification(it, asProto(it)) }
        val updates = Flux.from(tx.bus)

        return Flux.concat(current, updates)
                .doFinally { onFinished(tx.chain, tx.id) }
                .doOnNext { onSend(it.tx) }
                .map { it.proto }
    }

    fun add(requestMono: Mono<BlockchainOuterClass.TxStatusRequest>): Flux<BlockchainOuterClass.TxStatus> {
        return requestMono.map { request ->
            prepareTracking(request)
        }.flatMapMany { tx ->
            streamAllUpdates(tx)
        }
    }

    private fun verifyAll(chain: Chain) {
        verifyAll(trackedForChain(chain)!!)
    }

    private fun verifyAll(list: Collection<TxDetails>) {
        list
                .toFlux()
                .parallel(8).runOn(upstreamScheduler)
                .flatMap { checkForUpdate(it) }
                .sequential()
                .map { Tuples.of(it, asProto(it)) }
                .subscribe { t ->
                    notify(t.t1.bus, t.t2, t.t1)
                }
    }

    fun setBlockDetails(tx: TxDetails, block: BlockJson<TransactionId>): TxDetails {
        return if (block.number != null && block.totalDifficulty != null) {
            tx.withStatus(
                    blockTotalDifficulty = block.totalDifficulty,
                    blockTime = block.timestamp.toInstant()
            )
        } else {
            tx.withStatus(
                    mined = false
            )
        }
    }

    private fun loadWeight(tx: TxDetails): Mono<TxDetails> {
        val upstream = upstreams.getUpstream(tx.chain)
                ?: return Mono.error(Exception("Unsupported blockchain: ${tx.chain}"))
        return upstream.getApi(Selector.empty)
                .executeAndConvert(Commands.eth().getBlock(tx.status.blockHash))
                .map { block ->
                    setBlockDetails(tx, block)
                }.doOnError { t ->
                    log.warn("Failed to update weight", t)
                }
    }

    fun updateFromBlock(upstream: Upstream, tx: TxDetails, it: TransactionJson): Mono<TxDetails> {
        return if (it.blockNumber != null && it.blockHash != null && it.blockHash != ZERO_BLOCK) {
            val updated = tx.withStatus(
                    blockHash = it.blockHash,
                    height = it.blockNumber,
                    found = true,
                    mined = true,
                    confirmations = 1
            )
            upstream.getHead().getHead().map { head ->
                val height = updated.status.height
                if (height == null || head.number < height) {
                    updated
                } else {
                    updated.withStatus(
                            confirmations = head.number - height + 1
                    )
                }
            }.doOnError { t ->
                log.error("Unable to load head details", t)
            }.flatMap(this::loadWeight)
        } else {
            Mono.just(tx.withStatus(
                    found = true,
                    mined = false
            ))
        }
    }

    private fun checkForUpdate(tx: TxDetails): Mono<TxDetails> {
        val initialStatus = tx.status
        val upstream = upstreams.getUpstream(tx.chain) ?: return Mono.error(Exception("Unsupported blockchain: ${tx.chain}"))
        val execution = upstream.getApi(Selector.empty)
                .executeAndConvert(Commands.eth().getTransaction(tx.txid))
        return execution
                .flatMap { updateFromBlock(upstream, tx, it) }
                .switchIfEmpty(Mono.just(tx.withStatus(found = false)))
                .filter { current ->
                    initialStatus != current.status || current.shouldNotify() || current.shouldClose()
                }
    }

    private fun asProto(tx: TxDetails): BlockchainOuterClass.TxStatus {
        val data = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(tx.txid.toHex())
                .setConfirmations(tx.status.confirmations.toInt())

        data.broadcasted = tx.status.found
        val isMined = tx.status.mined
        data.mined = isMined
        if (isMined) {
            data.setBlock(
                    Common.BlockInfo.newBuilder()
                            .setBlockId(tx.status.blockHash!!.toHex().substring(2))
                            .setTimestamp(tx.status.blockTime!!.toEpochMilli())
                            .setWeight(ByteString.copyFrom(tx.status.blockTotalDifficulty!!.toByteArray()))
                            .setHeight(tx.status.height!!)
            )
        }
        return data.build()
    }

    private fun notify(client: TopicProcessor<Notification>, data: BlockchainOuterClass.TxStatus, tx: TxDetails) {
        try {
            client.onNext(Notification(tx, data))
        } catch (t: Throwable) {
            log.warn("Failed to put to bus", t)
        }
    }

    class Notification(val tx: TxDetails, val proto: BlockchainOuterClass.TxStatus)

    class TrackedTx(var tx: TxDetails)

    class TxDetails(val chain: Chain,
                    val bus: TopicProcessor<Notification>,
                    val since: Instant,
                    val txid: TransactionId,
                    val maxConfirmations: Int,
                    val id: Long,
                    val backref: TrackedTx? = null,
                    val status: TxStatus = TxStatus(),
                    val notifiedAt: Instant = Instant.now().minus(Period.ofDays(1))
    ) {

        fun copy(
                since: Instant = this.since,
                backref: TrackedTx? = this.backref,
                status: TxStatus = this.status,
                notifiedAt: Instant = this.notifiedAt
        ) = TxDetails(chain, bus, since, txid, maxConfirmations, id, backref, status, notifiedAt)

        fun withStatus(found: Boolean = this.status.found,
                       height: Long? = this.status.height,
                       mined: Boolean = this.status.mined,
                       blockHash: BlockHash? = this.status.blockHash,
                       blockTime: Instant? = this.status.blockTime,
                       blockTotalDifficulty: BigInteger? = this.status.blockTotalDifficulty,
                       confirmations: Long = this.status.confirmations): TxDetails {
            return copy(status = this.status.copy(found, height, mined, blockHash, blockTime, blockTotalDifficulty, confirmations))
        }

        fun withCleanStatus(): TxDetails {
            return copy(status = this.status.clean())
        }

        fun shouldClose(): Boolean {
            return maxConfirmations <= this.status.confirmations
                    || since.isBefore(Instant.now().minus(TRACK_TTL))
                    || (!status.found && since.isBefore(Instant.now().minus(NOT_FOUND_TRACK_TTL)))
                    || (!status.mined && since.isBefore(Instant.now().minus(NOT_MINED_TRACK_TTL)))
        }

        fun shouldNotify(): Boolean {
            return this.notifiedAt.isBefore(Instant.now().minus(PING_PERIOD))
        }

        fun justNotified(): TxDetails {
            return notifiedAt(Instant.now())
        }

        fun notifiedAt(time: Instant): TxDetails {
            return copy(notifiedAt = time)
        }

        fun withBackref(backref: TrackedTx): TxDetails {
            return copy(backref = backref)
        }

        fun makeCurrent(): TxDetails {
            backref?.tx = this
            return this
        }
    }

    class TxStatus(val found: Boolean = false,
                   val height: Long? = null,
                   val mined: Boolean = false,
                   val blockHash: BlockHash? = null,
                   val blockTime: Instant? = null,
                   val blockTotalDifficulty: BigInteger? = null,
                   val confirmations: Long = 0) {

        fun copy(found: Boolean = this.found,
                 height: Long? = this.height,
                 mined: Boolean = this.mined,
                 blockHash: BlockHash? = this.blockHash,
                 blockTime: Instant? = this.blockTime,
                 blockTotalDifficulty: BigInteger? = this.blockTotalDifficulty,
                 confirmation: Long = this.confirmations)
                = TxStatus(found, height, mined, blockHash, blockTime, blockTotalDifficulty, confirmation)

        fun clean() = TxStatus(false, null, false, null, null, null, 0)

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as TxStatus

            if (found != other.found) return false
            if (height != other.height) return false
            if (mined != other.mined) return false
            if (blockHash != other.blockHash) return false
            if (blockTime != other.blockTime) return false
            if (blockTotalDifficulty != other.blockTotalDifficulty) return false
            if (confirmations != other.confirmations) return false

            return true
        }

        override fun hashCode(): Int {
            var result = found.hashCode()
            result = 31 * result + (height?.hashCode() ?: 0)
            result = 31 * result + (blockHash?.hashCode() ?: 0)
            return result
        }

    }
}