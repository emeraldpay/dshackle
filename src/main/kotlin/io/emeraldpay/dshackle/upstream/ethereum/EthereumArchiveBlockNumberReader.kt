package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.ethereum.hex.HexQuantity
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Mono
import kotlin.math.max

private const val ARBITRUM_NITRO_BLOCK = "0x152DD47" // 22207815
private const val OPTIMISM_BEDROCK_BLOCK = "0x645C277" // 105235063
private const val EARLIEST_BLOCK = "0x2710" // 10000

class EthereumArchiveBlockNumberReader(
    private val reader: ChainReader,
) {

    fun readArchiveBlock(): Mono<String> =
        reader.read(ChainRequest("eth_blockNumber", ListParams()))
            .flatMap(ChainResponse::requireResult)
            .map {
                HexQuantity
                    .from(
                        max(1, String(it).substring(3, it.size - 1).toLong(radix = 16) - 10_000), // this is definitely archive
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
