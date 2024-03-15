package io.emeraldpay.dshackle.upstream.beaconchain

import com.fasterxml.jackson.databind.node.NullNode
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.upstream.BasicEthLabelsDetector
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.rpcclient.RestParams
import reactor.core.publisher.Flux

class BeaconChainLabelsDetector(
    reader: ChainReader,
) : BasicEthLabelsDetector(reader) {

    override fun nodeTypeRequest(): NodeTypeRequest {
        return NodeTypeRequest(
            ChainRequest("GET#/eth/v1/node/version", RestParams.emptyParams()),
        ) { node ->
            node.get("data")?.get("version") ?: NullNode.instance
        }
    }

    override fun detectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
        )
    }
}
