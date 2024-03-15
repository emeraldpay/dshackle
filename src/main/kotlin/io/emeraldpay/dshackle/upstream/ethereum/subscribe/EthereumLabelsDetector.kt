package io.emeraldpay.dshackle.upstream.ethereum.subscribe

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BasicEthLabelsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.ethereum.EthereumArchiveBlockNumberReader
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

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
                ListParams("0x756F45E3FA69347A9A973A725E3C98bC4db0b5a0", blockNumber),
            ),
        ).flatMap(ChainResponse::requireResult)
    }

    override fun nodeTypeRequest(): NodeTypeRequest {
        return NodeTypeRequest(
            ChainRequest("web3_clientVersion", ListParams()),
        ) { node -> node }
    }
}
