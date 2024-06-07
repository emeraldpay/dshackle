package io.emeraldpay.dshackle.upstream

import com.fasterxml.jackson.core.type.TypeReference
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

typealias UpstreamRpcModulesDetectorBuilder = (Upstream) -> UpstreamRpcModulesDetector?

abstract class UpstreamRpcModulesDetector(
    private val upstream: Upstream,
) {
    protected val log: Logger = LoggerFactory.getLogger(this::class.java)

    open fun detectRpcModules(): Mono<HashMap<String, String>> {
        return upstream.getIngressReader()
            .read(rpcModulesRequest())
            .flatMap(ChainResponse::requireResult)
            .map(::parseRpcModules)
            .onErrorResume {
                log.warn("Can't detect rpc_modules of upstream ${upstream.getId()}, reason - {}", it.message)
                Mono.just(HashMap())
            }
    }

    protected abstract fun rpcModulesRequest(): ChainRequest

    protected abstract fun parseRpcModules(data: ByteArray): HashMap<String, String>
}

class BasicEthUpstreamRpcModulesDetector(
    upstream: Upstream,
) : UpstreamRpcModulesDetector(upstream) {
    override fun rpcModulesRequest(): ChainRequest = ChainRequest("rpc_modules", ListParams())

    override fun parseRpcModules(data: ByteArray): HashMap<String, String> {
        return Global.objectMapper.readValue(data, object : TypeReference<HashMap<String, String>>() {})
    }
}
