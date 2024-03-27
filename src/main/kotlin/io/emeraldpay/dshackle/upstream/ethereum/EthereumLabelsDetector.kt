package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BasicEthLabelsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

const val ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

class EthereumLabelsDetector(
    private val reader: ChainReader,
    private val chain: Chain,
) : BasicEthLabelsDetector(reader) {
    private val blockNumberReader = EthereumArchiveBlockNumberReader(reader)

    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
            detectArchiveNode(),
        )
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
            ChainRequest(
                "eth_getBalance",
                ListParams(ZERO_ADDRESS, blockNumber),
            ),
        ).flatMap(ChainResponse::requireResult)
    }

    override fun nodeTypeRequest(): NodeTypeRequest {
        return NodeTypeRequest(
            ChainRequest("web3_clientVersion", ListParams()),
        ) { node -> node }
    }
}
