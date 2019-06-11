package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.grpc.Chain
import io.grpc.stub.StreamObserver
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Batch
import io.infinitape.etherjar.rpc.Commands
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.kotlin.core.publisher.switchIfEmpty
import java.lang.Exception
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import javax.annotation.PostConstruct

@Service
class TrackTx(
        @Autowired private val upstreams: Upstreams
) {

    private val ZERO_BLOCK = BlockHash.from("0x0000000000000000000000000000000000000000000000000000000000000000")

    private val log = LoggerFactory.getLogger(TrackTx::class.java)
    private val clients = HashMap<Chain, ConcurrentLinkedQueue<TrackedTx>>()

    @PostConstruct
    fun init() {
        listOf(Chain.MORDEN, Chain.ETHEREUM_CLASSIC, Chain.ETHEREUM).forEach { chain ->
            clients[chain] = ConcurrentLinkedQueue()
            upstreams.ethereumUpstream(chain)?.head?.let { head ->
                head.getFlux().subscribe { verifyAll(chain) }
            }
        }
    }

    private fun currentList(chain: Chain): ConcurrentLinkedQueue<TrackedTx> {
        return clients[chain]!!
    }

    fun add(tx: TrackedTx) {
        currentList(tx.chain).add(tx)
        verify(tx)
        notify(tx)
    }

    private fun verifyAll(chain: Chain) {
        currentList(chain)
                .toFlux()
                .filter(this::verify)
                .subscribe {
                    notify(it)
                }
    }

    private fun verify(tx: TrackedTx): Boolean {
        val found = tx.status.found
        val mined = tx.status.mined
        val batch = Batch()
        val execution = Mono.fromCompletionStage(batch.add(Commands.eth().getTransaction(tx.txid)))
        val upstream = upstreams.ethereumUpstream(tx.chain)!!
        upstream.api.execute(batch)
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
                return@flatMap upstream.head.getHead().map { head ->
                    tx.withStatus(
                            confirmation = head.number - tx.status.height!! + 1,
                            blockTime = head.timestamp.toInstant()
                    )
                }
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

    private fun notify(tx: TrackedTx): Boolean {
        val client = tx.stream
        val data = BlockchainOuterClass.TxStatus.newBuilder()
                .setChainValue(tx.chain.id)
                .setTxid(tx.txid.toHex())
                .setConfirmations(tx.status.confirmation.toInt())
                .setMined(tx.status.mined)
                .setBroadcasted(tx.status.found)

        if (tx.status.mined) {
            data.setBlock(
                    Common.BlockInfo.newBuilder()
                            .setHash(ByteString.copyFrom(tx.status.blockHash!!.bytes))
                            .setHeight(tx.status.height!!)
                            .setTimestamp(tx.status.blockTime!!.toEpochMilli())
            )
        }
        var sent: Boolean = false
        try {
            sent = client.send(data.build())
            if (!sent || tx.shouldClose()) {
                if (sent) {
                    client.stream.onCompleted()
                }
                currentList(tx.chain).remove(tx)
            }
        } catch (e: Exception) {
            log.error("Send error ${e.javaClass}: ${e.message}")
        }
        return sent
    }

    class TrackedTx(val chain: Chain,
                    val stream: StreamSender<BlockchainOuterClass.TxStatus>,
                    val since: Instant,
                    val txid: TransactionId,
                    val maxConfirmations: Int,
                    var status: TxStatus = TxStatus()) {

        fun withStatus(found: Boolean = this.status.found,
                       height: Long? = this.status.height,
                       mined: Boolean = this.status.mined,
                       blockHash: BlockHash? = this.status.blockHash,
                       blockTime: Instant? = this.status.blockTime,
                       confirmation: Long = this.status.confirmation): TrackedTx {
            this.status = this.status.copy(found, height, mined, blockHash, blockTime, confirmation)
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
                   var confirmation: Long = 0) {
        fun copy(found: Boolean = this.found,
                 height: Long? = this.height,
                 mined: Boolean = this.mined,
                 blockHash: BlockHash? = this.blockHash,
                 blockTime: Instant? = this.blockTime,
                 confirmation: Long = this.confirmation)
                = TxStatus(found, height, mined, blockHash, blockTime, confirmation)
    }

}