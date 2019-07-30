package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.AvailableChains
import io.emeraldpay.dshackle.upstream.ConfiguredUpstreams
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
import java.lang.Exception
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import javax.annotation.PostConstruct
import kotlin.math.max
import kotlin.math.min

@Service
class TrackTx(
        @Autowired private val upstreams: Upstreams,
        @Autowired private val availableChains: AvailableChains
) {

    private val ZERO_BLOCK = BlockHash.from("0x0000000000000000000000000000000000000000000000000000000000000000")

    private val log = LoggerFactory.getLogger(TrackTx::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedTx>>()

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
            val sender = TopicProcessor.create<BlockchainOuterClass.TxStatus>()
            TrackTx.TrackedTx(
                    Chain.byId(request.chainValue),
                    sender,
                    Instant.now(),
                    TransactionId.from(request.txId),
                    min(max(1, request.confirmationLimit), 100)
            )
        }.filter {
            clients.containsKey(it.chain)
        }.map { tx ->
            currentList(tx.chain)!!.let { list ->
                list.add(tx)
                tx.stream.doOnError {
                    list.remove(tx)
                    tx.stream.dispose()
                }
            }
            tx
        }.map { tx ->
            verify(tx)
            notify(tx)
            tx
        }.flatMapMany { tx ->
            tx.stream
        }
    }

    private fun verifyAll(chain: Chain) {
        currentList(chain)!!
                .toFlux()
                .filter(this::verify)
                .subscribe {
                    notify(it)
                }
    }

    private fun loadWeight(tx: TrackedTx): Mono<TrackedTx> {
        val upstream = upstreams.getUpstream(tx.chain)
                ?: return Mono.error(Exception("Unsupported blockchain: ${tx.chain}"))
        return upstream.getApi()
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
                }
    }

    private fun verify(tx: TrackedTx): Boolean {
        val found = tx.status.found
        val mined = tx.status.mined
        val upstream = upstreams.getUpstream(tx.chain) ?: return false
        val execution = upstream.getApi()
                .executeAndConvert(Commands.eth().getTransaction(tx.txid))
        val update = execution.flatMap {
            if (it.blockNumber != null
                    && it.blockHash != null && it.blockHash != ZERO_BLOCK) {
                tx.withStatus(
                        blockHash = it.blockHash,
                        height = it.blockNumber,
                        found = true,
                        mined = true,
                        confirmation = 1
                )
                return@flatMap upstream.getHead().getHead().map { head ->
                    tx.withStatus(
                            confirmation = head.number - tx.status.height!! + 1
                    )
                }.flatMap(this::loadWeight)
            } else {
                tx.withStatus(
                    found = true,
                    mined = false
                )
            }
            return@flatMap Mono.just(tx)
        }.block()
        if (update == null) {
            tx.withStatus(
                found = false,
                mined = false
            )
        }
        if (!found) {
            return tx.status.found != found
        }
        if (!mined) {
            return tx.status.mined != mined
        }
        return true
    }

    private fun notify(tx: TrackedTx) {
        val client = tx.stream
        val data = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(tx.txid.toHex())
                .setConfirmations(tx.status.confirmation.toInt())
                .setMined(tx.status.mined)
                .setBroadcasted(tx.status.found)

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
        client.onNext(data.build())
    }

    class TrackedTx(val chain: Chain,
                    val stream: TopicProcessor<BlockchainOuterClass.TxStatus>,
                    val since: Instant,
                    val txid: TransactionId,
                    val maxConfirmations: Int,
                    var status: TxStatus = TxStatus()) {

        fun withStatus(found: Boolean = this.status.found,
                       height: Long? = this.status.height,
                       mined: Boolean = this.status.mined,
                       blockHash: BlockHash? = this.status.blockHash,
                       blockTime: Instant? = this.status.blockTime,
                       blockTotalDifficulty: BigInteger? = this.status.blockTotalDifficulty,
                       confirmation: Long = this.status.confirmation): TrackedTx {
            this.status = this.status.copy(found, height, mined, blockHash, blockTime, blockTotalDifficulty, confirmation)
            return this
        }

        fun shouldClose(): Boolean {
            return maxConfirmations <= this.status.confirmation
                    || since.isBefore(Instant.now().minus(Duration.ofHours(1)))
        }
    }

    class TxStatus(var found: Boolean = false,
                   var height: Long? = null,
                   var mined: Boolean = false,
                   var blockHash: BlockHash? = null,
                   var blockTime: Instant? = null,
                   var blockTotalDifficulty: BigInteger? = null,
                   var confirmation: Long = 0) {
        fun copy(found: Boolean = this.found,
                 height: Long? = this.height,
                 mined: Boolean = this.mined,
                 blockHash: BlockHash? = this.blockHash,
                 blockTime: Instant? = this.blockTime,
                 blockTotalDifficulty: BigInteger? = this.blockTotalDifficulty,
                 confirmation: Long = this.confirmation)
                = TxStatus(found, height, mined, blockHash, blockTime, blockTotalDifficulty, confirmation)
    }

}