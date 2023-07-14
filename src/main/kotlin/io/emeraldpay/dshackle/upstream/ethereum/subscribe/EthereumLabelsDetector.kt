package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Global.Companion.objectMapper
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.etherjar.hex.HexQuantity
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class EthereumLabelsDetector(
    private val reader: JsonRpcReader
) {

    fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode()
        )
    }

    private fun detectNodeType(): Mono<Pair<String, String>?> {
        return reader
            .read(JsonRpcRequest("web3_clientVersion", listOf()))
            .flatMap(JsonRpcResponse::requireResult)
            .mapNotNull {
                val node = objectMapper.readValue<JsonNode>(it)
                if (node.isTextual) {
                    nodeType(node.textValue())?.run {
                        "client_type" to this
                    }
                } else {
                    null
                }
            }
            .onErrorResume { Mono.empty() }
    }

    private fun detectArchiveNode(): Mono<Pair<String, String>> {
        return reader
            .read(JsonRpcRequest("eth_blockNumber", listOf()))
            .flatMap(JsonRpcResponse::requireResult)
            .flatMap {
                val blockNum = HexQuantity.from(String(it).substring(3, it.size - 1).toLong(radix = 16) - 10_000) // this is definitely archive
                reader.read(
                    JsonRpcRequest(
                        "eth_getBalance",
                        listOf("0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", blockNum.toHex())
                    )
                )
            }
            .flatMap(JsonRpcResponse::requireResult)
            .map { "archive" to "true" }
            .onErrorResume { Mono.empty() }
    }

    private fun nodeType(nodeType: String): String? {
        return if (nodeType.contains("erigon", true)) {
            "erigon"
        } else if (nodeType.contains("geth", true)) {
            "geth"
        } else if (nodeType.contains("bor", true)) {
            "bor"
        } else if (nodeType.contains("nethermind", true)) {
            "nethermind"
        } else {
            null
        }
    }
}
