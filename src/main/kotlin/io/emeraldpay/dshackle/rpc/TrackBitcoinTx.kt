package io.emeraldpay.dshackle.rpc

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.bitcoin.DirectBitcoinApi
import io.emeraldpay.dshackle.upstream.bitcoin.BitcoinUpstream
import io.emeraldpay.dshackle.upstream.bitcoin.ExtractBlock
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import kotlin.math.max
import kotlin.math.min

@Service
class TrackBitcoinTx(
        @Autowired private val upstreams: Upstreams
) : TrackTx {

    companion object {
        private val log = LoggerFactory.getLogger(TrackBitcoinTx::class.java)
    }

    override fun isSupported(chain: Chain): Boolean {
        return BlockchainType.fromBlockchain(chain) == BlockchainType.BITCOIN && upstreams.isAvailable(chain)
    }

    override fun subscribe(request: BlockchainOuterClass.TxStatusRequest): Flux<BlockchainOuterClass.TxStatus> {
        val chain = Chain.byId(request.chainValue)
        val upstream = upstreams.getUpstream(chain)?.cast(BitcoinUpstream::class.java, DirectBitcoinApi::class.java)
                ?: return Flux.error(SilentException.UnsupportedBlockchain(chain))
        val txid = request.txId
        val confirmations = max(min(1, request.confirmationLimit), 12)
        return upstream.getApi(Selector.empty).flatMapMany { api ->
            subscribe(chain, api, upstream, txid)
        }.takeUntil { tx ->
            tx.confirmations >= confirmations
        }.map(this::asProto)
    }

    fun subscribe(chain: Chain, api: DirectBitcoinApi, upstream: BitcoinUpstream, txid: String): Flux<TxStatus> {
        return loadExisting(api, txid)
                .flatMapMany { status ->
                    if (status.mined) {
                        //Head almost always knows the current height, so it can continue with calculating confirmations
                        //without publishing an empty TxStatus first
                        continueWithMined(api, upstream, status)
                    } else {
                        loadMempool(upstream, txid)
                                .flatMapMany { tx ->
                                    val next = if (tx.found) {
                                        untilMined(upstream, tx)
                                    } else {
                                        untilFound(chain, api, upstream, txid)
                                    }
                                    //fist provide the current status, then updates
                                    Flux.concat(Mono.just(tx), next)
                                }
                    }
                }
    }

    fun continueWithMined(api: DirectBitcoinApi, upstream: BitcoinUpstream, status: TxStatus): Flux<TxStatus> {
        return api.getBlock(status.blockHash!!)
                .map { block ->
                    TxStatus(status.txid, true, ExtractBlock.getHeight(block), true, status.blockHash, ExtractBlock.getTime(block), ExtractBlock.getDifficulty(block))
                }.flatMapMany { tx ->
                    withConfirmations(upstream, tx)
                }
    }

    fun untilFound(chain: Chain, api: DirectBitcoinApi, upstream: BitcoinUpstream, txid: String): Flux<TxStatus> {
        return Flux.interval(Duration.ofSeconds(1))
                .take(Duration.ofMinutes(10))
                .flatMap { loadMempool(upstream, txid) }
                .skipUntil { it.found }
                .flatMap { subscribe(chain, api, upstream, txid) }
                .doOnError { t ->
                    log.error("Failed to wait until found", t)
                }
    }

    fun untilMined(upstream: BitcoinUpstream, tx: TxStatus): Mono<TxStatus> {
        return upstream.getHead().getFlux().flatMap {
            upstream.getApi(Selector.empty).flatMap { api ->
                loadExisting(api, tx.txid)
            }.filter { it.mined }
        }.single()
    }

    fun withConfirmations(upstream: BitcoinUpstream, tx: TxStatus): Flux<TxStatus> {
        return upstream.getHead().getFlux().map {
            tx.withHead(it.height)
        }
    }

    fun loadExisting(api: DirectBitcoinApi, txid: String): Mono<TxStatus> {
        val mined = api.getTx(txid)
        return mined.map {
            val block = it["blockhash"] as String?
            TxStatus(txid, found = true, mined = block != null, blockHash = block, height = ExtractBlock.getHeight(it))
        }
    }

    fun loadMempool(upstream: BitcoinUpstream, txid: String): Mono<TxStatus> {
        println("access: ${upstream.getData()}")
        val mempool = upstream.getData().getMempool().get()
        return mempool.map {
            if (it.contains(txid)) {
                TxStatus(txid, found = true, mined = false)
            } else {
                TxStatus(txid, found = false, mined = false)
            }
        }
    }

    private fun asProto(tx: TxStatus): BlockchainOuterClass.TxStatus {
        val data = BlockchainOuterClass.TxStatus.newBuilder()
                .setTxId(tx.txid)
                .setConfirmations(tx.confirmations.toInt())

        data.broadcasted = tx.found
        val isMined = tx.mined
        data.mined = isMined
        if (isMined) {
            data.setBlock(
                    Common.BlockInfo.newBuilder()
                            .setBlockId(tx.blockHash!!.substring(2))
                            .setTimestamp(tx.blockTime!!.toEpochMilli())
                            .setWeight(ByteString.copyFrom(tx.blockTotalDifficulty!!.toByteArray()))
                            .setHeight(tx.height!!)
            )
        }
        return data.build()
    }

    class TxStatus(
            val txid: String,
            val found: Boolean = false,
            val height: Long? = null,
            val mined: Boolean = false,
            val blockHash: String? = null,
            val blockTime: Instant? = null,
            val blockTotalDifficulty: BigInteger? = null,
            val confirmations: Long = 0) {

        fun withHead(headHeight: Long) = TxStatus(txid, found, height, mined, blockHash, blockTime, blockTotalDifficulty, headHeight - height!! + 1)
    }
}