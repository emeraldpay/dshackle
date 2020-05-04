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

import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.BlockchainType
import io.emeraldpay.dshackle.SilentException
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.upstream.AggregatedUpstream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.Upstreams
import io.emeraldpay.dshackle.upstream.ethereum.EthereumApi
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.BlockHash
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.Commands
import io.infinitape.etherjar.rpc.RpcException
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.json.TransactionJson
import io.infinitape.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import java.math.BigInteger
import java.time.Duration
import java.time.Instant
import kotlin.math.max
import kotlin.math.min

@Service
class TrackEthereumTx(
        @Autowired private val upstreams: Upstreams
) : TrackTx {

    companion object {
        private val ZERO_BLOCK = BlockHash.from("0x0000000000000000000000000000000000000000000000000000000000000000")
        private val TRACK_TTL = Duration.ofHours(1)
        private val NOT_FOUND_TRACK_TTL = Duration.ofMinutes(1)
        private val NOT_MINED_TRACK_TTL = NOT_FOUND_TRACK_TTL.multipliedBy(2)
    }

    var scheduler: Scheduler = Schedulers.elastic()

    private val log = LoggerFactory.getLogger(TrackEthereumTx::class.java)

    override fun isSupported(chain: Chain): Boolean {
        return BlockchainType.fromBlockchain(chain) == BlockchainType.ETHEREUM && upstreams.isAvailable(chain)
    }

    override fun subscribe(request: BlockchainOuterClass.TxStatusRequest): Flux<BlockchainOuterClass.TxStatus> {
        val base = prepareTracking(request)
        val up = upstreams.getUpstream(base.chain)?.castApi(EthereumApi::class.java)
                ?: return Flux.empty()
        return update(base)
                .defaultIfEmpty(base)
                .flatMapMany {
                    Flux.concat(Mono.just(it), subscribe(it, up))
                            .distinctUntilChanged(TxDetails::status)
                            .map(this@TrackEthereumTx::asProto)
                            .subscribeOn(scheduler)
                }
    }

    fun subscribe(base: TxDetails, up: Upstream<EthereumApi>): Flux<TxDetails> {
        var latestTx = base

        val untilFound = Mono.just(latestTx)
                .subscribeOn(scheduler)
                .map {
                    //replace with the latest value, it may be already found
                    latestTx
                }
                .flatMap { latest ->
                    if (!latest.status.found) {
                        update(latest).defaultIfEmpty(latestTx)
                    } else {
                        Mono.just(latest)
                    }
                }
                .flatMap { received ->
                    if (!received.status.found) {
                        Mono.error(SilentException("Retry not found"))
                    } else {
                        Mono.just(received)
                    }
                }
                .retryWhen(
                        Retry.fixedDelay(10, Duration.ofSeconds(2))
                )
                .onErrorResume { Mono.empty() }

        val inBlocks = up.getHead().getFlux()
                .subscribeOn(scheduler)
                .flatMap { block ->
                    onNewBlock(latestTx, block)
                }

        return Flux.merge(untilFound, inBlocks)
                .takeUntil(TxDetails::shouldClose)
                .doOnNext { newTx ->
                    latestTx = newTx
                }
    }

    fun onNewBlock(tx: TxDetails, block: BlockContainer): Mono<TxDetails> {
        val txid = TxId.from(tx.txid)
        if (!tx.status.mined) {
            val justMined = block.transactions.contains(txid)
            return if (justMined) {
                Mono.just(tx.withStatus(
                        mined = true,
                        found = true,
                        confirmations = 1,
                        height = block.height,
                        blockTime = block.timestamp,
                        blockTotalDifficulty = block.difficulty,
                        blockHash = BlockHash(block.hash.value)
                ))
            } else {
                update(tx)
            }
        } else {
            //verify if it's still on chain
            //TODO head is supposed to erase block when it was replaced, so can safely recalc here
            return update(tx)
        }
    }

    private fun update(tx: TxDetails): Mono<TxDetails> {
        val initialStatus = tx.status
        val upstream = upstreams.getUpstream(tx.chain) as AggregatedUpstream<EthereumApi>?
                ?: return Mono.error(SilentException.UnsupportedBlockchain(tx.chain))
        val execution = upstream.getApi(Selector.empty)
                .flatMap { api -> api.executeAndConvert(Commands.eth().getTransaction(tx.txid)) }
        return execution
                .onErrorResume(RpcException::class.java) { t ->
                    log.warn("Upstream error, ignoring. {}", t.rpcMessage)
                    Mono.empty<TransactionJson>()
                }
                .flatMap { updateFromBlock(upstream, tx, it) }
                .doOnError { t ->
                    log.error("Failed to load tx block", t)
                }
                .switchIfEmpty(Mono.just(tx.withStatus(found = false)))
                .filter { current ->
                    initialStatus != current.status || current.shouldClose()
                }
    }

    fun prepareTracking(request: BlockchainOuterClass.TxStatusRequest): TxDetails {
        val chain = Chain.byId(request.chainValue)
        if (!isSupported(chain)) {
            throw SilentException.UnsupportedBlockchain(request.chainValue)
        }
        val details = TxDetails(
                chain,
                Instant.now(),
                TransactionId.from(request.txId),
                min(max(1, request.confirmationLimit), 100)
        )
        return details
    }

    fun setBlockDetails(tx: TxDetails, block: BlockJson<TransactionRefJson>): TxDetails {
        return if (block.number != null && block.totalDifficulty != null) {
            tx.withStatus(
                    blockTotalDifficulty = block.totalDifficulty,
                    blockTime = block.timestamp
            )
        } else {
            tx.withStatus(
                    mined = false
            )
        }
    }

    private fun loadWeight(tx: TxDetails): Mono<TxDetails> {
        val upstream = upstreams.getUpstream(tx.chain) as AggregatedUpstream<EthereumApi>?
                ?: return Mono.error(SilentException.UnsupportedBlockchain(tx.chain))
        return upstream.getApi(Selector.empty)
                .flatMap { api -> api.executeAndConvert(Commands.eth().getBlock(tx.status.blockHash)) }
                .map { block ->
                    setBlockDetails(tx, block)
                }.doOnError { t ->
                    log.warn("Failed to update weight", t)
                }
    }

    fun updateFromBlock(upstream: Upstream<EthereumApi>, tx: TxDetails, blockTx: TransactionJson): Mono<TxDetails> {
        return if (blockTx.blockNumber != null && blockTx.blockHash != null && blockTx.blockHash != ZERO_BLOCK) {
            val updated = tx.withStatus(
                    blockHash = blockTx.blockHash,
                    height = blockTx.blockNumber,
                    found = true,
                    mined = true,
                    confirmations = 1
            )
            upstream.getHead().getFlux().next().map { head ->
                val height = updated.status.height
                if (height == null || head.height < height) {
                    updated
                } else {
                    updated.withStatus(
                            confirmations = head.height - height + 1
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

    class TxDetails(val chain: Chain,
                    val since: Instant,
                    val txid: TransactionId,
                    val maxConfirmations: Int,
                    val status: TxStatus
    ) {

        constructor(chain: Chain,
                    since: Instant,
                    txid: TransactionId,
                    maxConfirmations: Int) : this(chain, since, txid, maxConfirmations, TxStatus())

        fun copy(
                since: Instant = this.since,
                status: TxStatus = this.status
        ) = TxDetails(chain, since, txid, maxConfirmations, status)

        fun withStatus(found: Boolean = this.status.found,
                       height: Long? = this.status.height,
                       mined: Boolean = this.status.mined,
                       blockHash: BlockHash? = this.status.blockHash,
                       blockTime: Instant? = this.status.blockTime,
                       blockTotalDifficulty: BigInteger? = this.status.blockTotalDifficulty,
                       confirmations: Long = this.status.confirmations): TxDetails {
            return copy(status = this.status.copy(found, height, mined, blockHash, blockTime, blockTotalDifficulty, confirmations))
        }

        fun shouldClose(): Boolean {
            return maxConfirmations <= this.status.confirmations
                    || since.isBefore(Instant.now().minus(TRACK_TTL))
                    || (!status.found && since.isBefore(Instant.now().minus(NOT_FOUND_TRACK_TTL)))
                    || (!status.mined && since.isBefore(Instant.now().minus(NOT_MINED_TRACK_TTL)))
        }

        override fun toString(): String {
            return "TxDetails(chain=$chain, txid=$txid, status=$status)"
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is TxDetails) return false

            if (chain != other.chain) return false
            if (since != other.since) return false
            if (txid != other.txid) return false
            if (maxConfirmations != other.maxConfirmations) return false
            if (status != other.status) return false

            return true
        }

        override fun hashCode(): Int {
            var result = chain.hashCode()
            result = 31 * result + since.hashCode()
            result = 31 * result + txid.hashCode()
            result = 31 * result + status.hashCode()
            return result
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

        override fun toString(): String {
            return "TxStatus(found=$found, height=$height, mined=$mined, blockHash=$blockHash, blockTime=$blockTime, blockTotalDifficulty=$blockTotalDifficulty, confirmations=$confirmations)"
        }


    }
}