package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.Global
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

const val UNKNOWN_CLIENT_VERSION = "unknown"

typealias UpstreamSettingsDetectorBuilder = (Chain, Upstream) -> UpstreamSettingsDetector?
abstract class UpstreamSettingsDetector(
    private val upstream: Upstream,
) {
    protected val log = LoggerFactory.getLogger(this::class.java)

    abstract fun detectLabels(): Flux<Pair<String, String>>

    fun detectClientVersion(): Mono<String> {
        return upstream.getIngressReader()
            .read(clientVersionRequest())
            .flatMap(ChainResponse::requireResult)
            .map(::parseClientVersion)
            .onErrorResume {
                log.warn("Can't detect the client version of upstream ${upstream.getId()}, reason - {}", it.message)
                Mono.just(UNKNOWN_CLIENT_VERSION)
            }
    }

    protected abstract fun clientVersionRequest(): ChainRequest

    protected abstract fun parseClientVersion(data: ByteArray): String
}

abstract class BasicEthUpstreamSettingsDetector(
    private val upstream: Upstream,
) : UpstreamSettingsDetector(upstream) {
    protected abstract fun nodeTypeRequest(): NodeTypeRequest

    protected fun detectNodeType(): Flux<Pair<String, String>?> {
        val nodeTypeRequest = nodeTypeRequest()
        return upstream
            .getIngressReader()
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
