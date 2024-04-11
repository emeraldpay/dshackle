package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundDetector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import kotlin.math.max

class SolanaLowerBoundSlotDetector(
    private val upstream: Upstream,
) : LowerBoundDetector() {
    private val reader = upstream.getIngressReader()

    override fun period(): Long {
        return 3
    }

    override fun internalDetectLowerBound(): Flux<LowerBoundData> {
        return Mono.just(reader)
            .flatMap {
                it.read(
                    ChainRequest("getFirstAvailableBlock", ListParams()), // in case of solana we talk about the slot of the lowest confirmed block
                )
            }
            .flatMap(ChainResponse::requireResult)
            .map {
                val slot = String(it).toLong()
                if (slot == 0L) {
                    1L
                } else {
                    slot
                }
            }
            .flatMapMany {
                reader.read(
                    ChainRequest(
                        "getBlock", // since getFirstAvailableBlock returns the slot of the lowest confirmed block we can directly call getBlock
                        ListParams(
                            it,
                            mapOf(
                                "showRewards" to false,
                                "transactionDetails" to "none",
                                "maxSupportedTransactionVersion" to 0,
                            ),
                        ),
                    ),
                )
                    .flatMap(ChainResponse::requireResult)
                    .flatMapMany { blockData ->
                        val block = Global.objectMapper.readValue(blockData, SolanaBlock::class.java)
                        Flux.just(
                            LowerBoundData(max(block.height, 1), LowerBoundType.STATE),
                            LowerBoundData(it, LowerBoundType.SLOT),
                        )
                    }
            }
            .retryWhen(
                Retry
                    .backoff(20, Duration.ofSeconds(1))
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
}
