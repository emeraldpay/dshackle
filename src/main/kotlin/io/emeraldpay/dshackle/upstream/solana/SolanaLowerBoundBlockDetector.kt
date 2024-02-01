package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import kotlin.math.max

class SolanaLowerBoundBlockDetector(
    chain: Chain,
    private val upstream: Upstream,
) : LowerBoundBlockDetector(chain, upstream) {
    private val reader = upstream.getIngressReader()

    override fun lowerBlockDetect(): Mono<LowerBlockData> {
        return Mono.just(reader)
            .flatMap {
                it.read(
                    JsonRpcRequest("getFirstAvailableBlock", listOf()), // in case of solana we talk about the slot of the lowest confirmed block
                )
            }
            .flatMap(JsonRpcResponse::requireResult)
            .map {
                val slot = String(it).toLong()
                if (slot == 0L) {
                    1L
                } else {
                    slot
                }
            }
            .flatMap { slot ->
                val from = if (slot <= 10) {
                    1
                } else {
                    slot - 10
                }
                reader.read(
                    JsonRpcRequest(
                        "getBlocks",
                        listOf(
                            from,
                            slot,
                        ),
                    ),
                )
            }
            .flatMap(JsonRpcResponse::requireResult)
            .flatMap {
                val response = Global.objectMapper.readValue(it, LongArray::class.java)
                if (response == null || response.isEmpty()) {
                    Mono.empty()
                } else {
                    val maxSlot = response.max()
                    reader.read(
                        JsonRpcRequest(
                            "getBlock",
                            listOf(
                                maxSlot,
                                mapOf(
                                    "showRewards" to false,
                                    "transactionDetails" to "none",
                                    "maxSupportedTransactionVersion" to 0,
                                ),
                            ),
                        ),
                    )
                        .flatMap(JsonRpcResponse::requireResult)
                        .map { blockData ->
                            val block = Global.objectMapper.readValue(blockData, SolanaBlock::class.java)
                            LowerBlockData(max(block.height, 1), maxSlot)
                        }
                }
            }
            .retryWhen(
                Retry
                    .backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                    .maxBackoff(Duration.ofMinutes(3))
                    .doAfterRetry {
                        log.debug(
                            "Error in calculation of lower block of upstream {}, retry attempt - {}, message - {}",
                            upstream.getId(),
                            it.totalRetries(),
                            it.failure().message,
                        )
                    },
            )
    }

    override fun periodRequest(): Long {
        return 3
    }
}
