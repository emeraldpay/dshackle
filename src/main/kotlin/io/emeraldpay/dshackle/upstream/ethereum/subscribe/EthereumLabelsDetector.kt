package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global.Companion.objectMapper
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.LabelsDetector
import io.emeraldpay.dshackle.upstream.ethereum.EthereumArchiveBlockNumberReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class EthereumLabelsDetector(
    private val reader: JsonRpcReader,
    private val chain: Chain,
) : LabelsDetector {
    private val blockNumberReader = EthereumArchiveBlockNumberReader(reader)

    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode(),
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
        return Mono.zip(
            blockNumberReader.readEarliestBlock(chain).flatMap { haveBalance(it) },
            blockNumberReader.readArchiveBlock().flatMap { haveBalance(it) },
        )
            .map { "archive" to "true" }
            .onErrorResume { Mono.empty() }
    }

    private fun haveBalance(blockNumber: String): Mono<ByteArray> {
        return reader.read(
            JsonRpcRequest(
                "eth_getBalance",
                listOf("0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", blockNumber),
            ),
        ).flatMap(JsonRpcResponse::requireResult)
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
