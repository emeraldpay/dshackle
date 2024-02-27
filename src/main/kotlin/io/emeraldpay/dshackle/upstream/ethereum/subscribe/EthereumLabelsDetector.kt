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
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class EthereumLabelsDetector(
    private val reader: JsonRpcReader,
    private val chain: Chain,
) : LabelsDetector {
    private val blockNumberReader = EthereumArchiveBlockNumberReader(reader)

    companion object {
        private val log = LoggerFactory.getLogger(EthereumLabelsDetector::class.java)
    }

    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode(),
        )
    }

    private fun detectNodeType(): Flux<Pair<String, String>?> {
        return reader
            .read(JsonRpcRequest("web3_clientVersion", ListParams()))
            .flatMap(JsonRpcResponse::requireResult)
            .map { objectMapper.readValue<JsonNode>(it) }
            .flatMapMany { node ->
                val labels = mutableListOf<Pair<String, String>>()
                if (node.isTextual) {
                    clientType(node.textValue())?.let {
                        labels.add("client_type" to it)
                    }
                    clientVersion(node.textValue())?.let {
                        labels.add("client_version" to it)
                    }
                }

                Flux.fromIterable(labels)
            }
            .onErrorResume { Flux.empty() }
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
                ListParams("0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", blockNumber),
            ),
        ).flatMap(JsonRpcResponse::requireResult)
    }

    private fun clientVersion(client: String): String? {
        val firstSlash = client.indexOf("/")
        val secondSlash = client.indexOf("/", firstSlash + 1)
        if (firstSlash == -1 || secondSlash == -1 || secondSlash < firstSlash) {
            return null
        }
        return client.substring(firstSlash + 1, secondSlash)
    }

    private fun clientType(client: String): String? {
        return if (client.contains("erigon", true)) {
            "erigon"
        } else if (client.contains("geth", true)) {
            "geth"
        } else if (client.contains("bor", true)) {
            "bor"
        } else if (client.contains("nethermind", true)) {
            "nethermind"
        } else {
            log.debug("Unknown client type: {}", client)
            null
        }
    }
}
