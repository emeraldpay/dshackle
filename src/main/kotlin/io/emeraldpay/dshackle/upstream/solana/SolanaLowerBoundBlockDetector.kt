package io.emeraldpay.dshackle.upstream.solana

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.LowerBoundBlockDetector
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono

class SolanaLowerBoundBlockDetector(
    chain: Chain,
    upstream: Upstream,
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
                String(it).toLong()
            }
            .flatMap { slot ->
                reader.read(
                    JsonRpcRequest(
                        "getBlocks",
                        listOf(
                            slot - 10,
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
                            LowerBlockData(block.height, maxSlot)
                        }.onErrorResume {
                            Mono.empty()
                        }
                }
            }
            .onErrorResume {
                Mono.empty()
            }
    }
}
