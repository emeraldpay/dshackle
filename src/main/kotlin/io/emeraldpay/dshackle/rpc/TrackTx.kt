package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.AvailableChains
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Commands
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.PostConstruct
import kotlin.math.max
import kotlin.math.min

@Service
class TrackTx(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val availableChains: AvailableChains,
        @Autowired private val upstreamScheduler: Scheduler
) {

    private val ZERO_BLOCK = BlockHash.from("0x0000000000000000000000000000000000000000000000000000000000000000")

    private val log = LoggerFactory.getLogger(TrackTx::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedTx>>()
    private val seq = AtomicLong(0)

    @PostConstruct
    fun init() {
        availableChains.observe().subscribe { chain ->
            clients[chain] = ConcurrentLinkedQueue()
            upstreams.getUpstream(chain)?.getHead()?.let { head ->
                head.getFlux().subscribe { verifyAll(chain) }
            }
        }
    }

    private fun currentList(chain: Chain): ConcurrentLinkedQueue<TrackedTx>? {
        return clients[chain]
    }

    fun add(requestMono: Mono<BlockchainOuterClass.TxStatusRequest>): Flux<BlockchainOuterClass.TxStatus> {
        return requestMono.map { request ->
            val bus = TopicProcessor.create<BlockchainOuterClass.TxStatus>()
            TrackTx.TrackedTx(
                    Chain.byId(request.chainValue),
                    bus,
                    Instant.now(),
                    TransactionId.from(request.txId),
                    min(max(1, request.confirmationLimit), 100),
                    seq.incrementAndGet()
            )
        }.filter {
            clients.containsKey(it.chain)
        }.flatMapMany { tx ->
            val current = checkForUpdate(tx).doOnNext{
                currentList(tx.chain)?.add(tx)
            }.map(this::asProto)

            val next = Flux.from(tx.bus)
            Flux.merge(current, next).doFinally {
                currentList(tx.chain)?.removeIf { x -> x.id == tx.id }
            }.doOnNext { txp ->
                if (txp.confirmations >= tx.maxConfirmations) {
                    tx.bus.onComplete()
                }
            }
        }
    }

    private fun verifyAll(chain: Chain) {
        currentList(chain)!!
                .toFlux()
                .parallel(8).runOn(upstreamScheduler)
                .flatMap {  checkForUpdate(it) }
                .sequential()
                .map { Tuples.of(it.bus, asProto(it)) }
                .subscribe { t ->
                    notify(t.t1, t.t2)
                }
    }

    private fun loadWeight(tx: TrackedTx): Mono<TrackedTx> {
        val upstream = upstreams.getUpstream(tx.chain)
                ?: return Mono.error(Exception("Unsupported blockchain: ${tx.chain}"))
        return upstream.getApi(Selector.empty)
                .executeAndConvert(Commands.eth().getBlock(tx.status.blockHash))
                .map { block ->
                    if (block != null && block.number != null && block.totalDifficulty != null) {
                        tx.withStatus(
                                blockTotalDifficulty = block.totalDifficulty,
                                blockTime = block.timestamp.toInstant()
                        )
                    } else {
                        tx.withStatus(
                                mined = false
                        )
                    }
                }.doOnError { t ->
                    log.warn("Failed to update weight", t)
                }
    }

    private fun checkForUpdate(tx: TrackedTx): Mono<TrackedTx> {
        val upstream = upstreams.getUpstream(tx.chain) ?: return Mono.error(Exception("Unsupported blockchain: ${tx.chain}"))
        val execution = upstream.getApi(Selector.empty)
                .executeAndConvert(Commands.eth().getTransaction(tx.txid))
        return execution.flatMap {
            if (it.blockNumber != null && it.blockHash != null && it.blockHash != ZERO_BLOCK) {
                val updated = tx.withStatus(
                        blockHash = it.blockHash,
                        height = it.blockNumber,
                        found = true,
                        mined = true,
                        confirmations = 1
                )
                upstream.getHead().getHead().map { head ->
                    if (updated.status.height == null || head.number < updated.status.height) {
                        updated
                    } else {
                        updated.withStatus(
                                confirmations = head.number - updated.status.height + 1
                        )
                    }
                }.flatMap(this::loadWeight)
            } else {
                Mono.just(tx.withStatus(
                        found = true,
                        mined = false
                ))
            }
        }.switchIfEmpty(Mono.just(tx.withStatus(found = false))).filter { current ->
            current.status != tx.status
        }
    }

    private fun asProto(tx: TrackedTx): BlockchainOuterClass.TxStatus {
        val data = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(tx.txid.toHex())
                .setConfirmations(tx.status.confirmations.toInt())

        if (tx.status.found != null) {
            data.broadcasted = tx.status.found
        }
        if (tx.status.mined != null) {
            data.mined = tx.status.mined
            if (tx.status.mined) {
                data.setBlock(
                        Common.BlockInfo.newBuilder()
                                .setBlockId(tx.status.blockHash!!.toHex().substring(2))
                                .setTimestamp(tx.status.blockTime!!.toEpochMilli())
                                .setWeight(ByteString.copyFrom(tx.status.blockTotalDifficulty!!.toByteArray()))
                                .setHeight(tx.status.height!!)
                                .setTimestamp(tx.status.blockTime!!.toEpochMilli())
                )
            }
        }
        return data.build()
    }

    private fun notify(client: TopicProcessor<BlockchainOuterClass.TxStatus>, data: BlockchainOuterClass.TxStatus) {
        client.onNext(data)
    }

    class TrackedTx(val chain: Chain,
                    val bus: TopicProcessor<BlockchainOuterClass.TxStatus>,
                    val since: Instant,
                    val txid: TransactionId,
                    val maxConfirmations: Int,
                    val id: Long,
                    val status: TxStatus = TxStatus()) {

        fun withStatus(found: Boolean? = this.status.found,
                       height: Long? = this.status.height,
                       mined: Boolean? = this.status.mined,
                       blockHash: BlockHash? = this.status.blockHash,
                       blockTime: Instant? = this.status.blockTime,
                       blockTotalDifficulty: BigInteger? = this.status.blockTotalDifficulty,
                       confirmations: Long = this.status.confirmations)
                = TrackedTx(
                        chain, bus, since, txid, maxConfirmations, id,
                        this.status.copy(found, height, mined, blockHash, blockTime, blockTotalDifficulty, confirmations)
                )

        fun withCleanStatus()
                = TrackedTx(
                        chain, bus, since, txid, maxConfirmations, id, this.status.clean()
                )

        fun shouldClose(): Boolean {
            return maxConfirmations <= this.status.confirmations
                    || since.isBefore(Instant.now().minus(Duration.ofHours(1)))
        }
    }

    class TxStatus(val found: Boolean? = null,
                   val height: Long? = null,
                   val mined: Boolean? = null,
                   val blockHash: BlockHash? = null,
                   val blockTime: Instant? = null,
                   val blockTotalDifficulty: BigInteger? = null,
                   val confirmations: Long = 0) {

        fun copy(found: Boolean? = this.found,
                 height: Long? = this.height,
                 mined: Boolean? = this.mined,
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