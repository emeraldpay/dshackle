package io.emeraldpay.dshackle.upstream.starknet

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetector
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Flux

class StarknetUpstreamSettingsDetector(
    private val upstream: Upstream,
    private val chain: Chain,
) : UpstreamSettingsDetector(upstream) {
    override fun internalDetectLabels(): Flux<Pair<String, String>> {
        return Flux.merge(
            detectNodeType(),
        )
    }

    private fun detectNodeType(): Flux<Pair<String, String>?> {
        return upstream
            .getIngressReader()
            .read(pathfinderVersionRequest())
            .flatMap(ChainResponse::requireResult)
            .flatMapMany { data ->
                val version = parseClientVersion(data)
                if (version.isEmpty()) {
                    throw Exception()
                }
                val labels =
                    mutableListOf("client_type" to "pathfinder", "client_version" to version)
                Flux.fromIterable(labels)
            }.onErrorResume {
                upstream
                    .getIngressReader()
                    .read(clientVersionRequest())
                    .flatMap(ChainResponse::requireResult)
                    .flatMapMany { data ->
                        val version = parseClientVersion(data)
                        if (version.isEmpty()) {
                            throw Exception()
                        }
                        val labels =
                            mutableListOf(
                                "client_type" to "juno",
                                "client_version" to version,
                            )
                        Flux.fromIterable(labels)
                    }
                    .onErrorResume { error ->
                        log.warn(
                            "Can't detect the node type of upstream ${upstream.getId()}, reason - {}",
                            error.message,
                        )
                        Flux.empty()
                    }
            }
    }

    override fun clientVersionRequest(): ChainRequest = ChainRequest("juno_version", ListParams())

    private fun pathfinderVersionRequest() = ChainRequest("pathfinder_version", ListParams())

    override fun parseClientVersion(data: ByteArray): String {
        var version = String(data)
        if (version.startsWith("\"") && version.endsWith("\"")) {
            version = version.substring(1, version.length - 1)
        }
        if (version.startsWith("v")) {
            version = version.substring(1, version.length)
        }
        return version
    }
}
