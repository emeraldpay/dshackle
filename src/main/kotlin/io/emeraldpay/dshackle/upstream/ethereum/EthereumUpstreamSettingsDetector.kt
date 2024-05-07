package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.databind.JsonNode
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.BasicEthUpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.NodeTypeRequest
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

const val ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

class EthereumUpstreamSettingsDetector(
    private val upstream: Upstream,
    private val chain: Chain,
) : BasicEthUpstreamSettingsDetector(upstream) {
    private val blockNumberReader = EthereumArchiveBlockNumberReader(upstream.getIngressReader())

    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode(),
        )
    }

    override fun mapping(node: JsonNode): String {
        return node.asText()
    }

    override fun clientVersionRequest(): ChainRequest {
        return ChainRequest("web3_clientVersion", ListParams())
    }

    override fun parseClientVersion(data: ByteArray): String {
        val version = String(data)
        if (version.startsWith("\"") && version.endsWith("\"")) {
            return version.substring(1, version.length - 1)
        }
        return version
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
        return upstream.getIngressReader().read(
            ChainRequest(
                "eth_getBalance",
                ListParams(ZERO_ADDRESS, blockNumber),
            ),
        ).flatMap(ChainResponse::requireResult)
    }

    override fun nodeTypeRequest(): NodeTypeRequest = NodeTypeRequest(clientVersionRequest())
}
