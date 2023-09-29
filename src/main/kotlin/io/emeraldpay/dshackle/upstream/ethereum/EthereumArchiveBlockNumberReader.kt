package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.hex.HexQuantity
import reactor.core.publisher.Mono

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
}
