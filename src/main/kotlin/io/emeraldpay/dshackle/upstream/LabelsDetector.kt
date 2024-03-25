package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.reader.ChainReader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

typealias LabelsDetectorBuilder = (Chain, ChainReader) -> LabelsDetector?
interface LabelsDetector {
    fun detectLabels(): Flux<Pair<String, String>>
}

abstract class BasicEthLabelsDetector(
    private val reader: ChainReader,
) : LabelsDetector {
    private val log = LoggerFactory.getLogger(this::class.java)

    protected abstract fun nodeTypeRequest(): NodeTypeRequest

    protected fun detectNodeType(): Flux<Pair<String, String>?> {
        val nodeTypeRequest = nodeTypeRequest()
        return reader
            .read(nodeTypeRequest.request)
            .flatMap(ChainResponse::requireResult)
            .map { Global.objectMapper.readValue<JsonNode>(it) }
            .flatMapMany { node ->
                val mappedNode = nodeTypeRequest.mapper(node)
                val labels = mutableListOf<Pair<String, String>>()
                if (mappedNode.isTextual) {
                    clientType(mappedNode.textValue())?.let {
                        labels.add("client_type" to it)
                    }
                    clientVersion(mappedNode.textValue())?.let {
                        labels.add("client_version" to it)
                    }
                }

                Flux.fromIterable(labels)
            }
            .onErrorResume {
                Flux.empty()
            }
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
        val firstSlash = client.indexOf("/")
        if (firstSlash == -1) {
            log.debug("Unknown client type: {}", client)
            return null
        }
        return client.substring(0, firstSlash).lowercase()
    }

    data class NodeTypeRequest(
        val request: ChainRequest,
        val mapper: (JsonNode) -> JsonNode,
    )
}
