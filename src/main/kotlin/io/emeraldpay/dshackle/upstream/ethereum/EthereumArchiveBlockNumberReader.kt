package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexQuantity
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Mono

private const val ARBITRUM_NITRO_BLOCK = "0x152DD47" // 22207815
private const val OPTIMISM_BEDROCK_BLOCK = "0x645C277" // 105235063
private const val EARLIEST_BLOCK = "0x2710" // 10000

class EthereumArchiveBlockNumberReader(
    private val reader: JsonRpcReader,
) {

    fun readArchiveBlock(): Mono<String> =
        reader.read(JsonRpcRequest("eth_blockNumber", listOf()))
            .flatMap(JsonRpcResponse::requireResult)
            .map {
                HexQuantity
                    .from(
                        String(it).substring(3, it.size - 1).toLong(radix = 16) - 10_000, // this is definitely archive
                    ).toHex()
            }

    fun readEarliestBlock(chain: Chain): Mono<String> {
        return when (chain) {
            Chain.ARBITRUM__MAINNET -> ARBITRUM_NITRO_BLOCK
            Chain.OPTIMISM__MAINNET -> OPTIMISM_BEDROCK_BLOCK
            else -> EARLIEST_BLOCK
        }.run { Mono.just(this) }
    }
}
