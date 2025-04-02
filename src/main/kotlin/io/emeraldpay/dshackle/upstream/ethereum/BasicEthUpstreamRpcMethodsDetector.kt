package io.emeraldpay.dshackle.upstream.ethereum

import com.fasterxml.jackson.core.type.TypeReference
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamRpcMethodsDetector
import io.emeraldpay.dshackle.upstream.calls.DefaultEthereumMethods
import io.emeraldpay.dshackle.upstream.rpcclient.CallParams
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Mono

class BasicEthUpstreamRpcMethodsDetector(
    private val upstream: Upstream,
    private val config: UpstreamsConfig.Upstream<*>,
) : UpstreamRpcMethodsDetector(upstream) {
    override fun detectByMagicMethod(): Mono<Map<String, Boolean>> =
        upstream
            .getIngressReader()
            .read(ChainRequest("rpc_modules", ListParams()))
            .flatMap(ChainResponse::requireResult)
            .map(::parseRpcModules)
            // force check all methods from rpcMethods
            .zipWith(detectByMethod()) { a, b ->
                a.plus(b)
            }.onErrorResume {
                log.warn("Can't detect rpc_modules of upstream ${upstream.getId()}, reason - {}", it.message)
                Mono.empty()
            }

    override fun rpcMethods(): Set<Pair<String, CallParams>> =
        setOf(
            "eth_getBlockReceipts" to ListParams("latest"),
            "trace_callMany" to ListParams(listOf(listOf<Any>())),
            "eth_simulateV1" to ListParams(listOf(listOf<Any>())),
        )

    private fun parseRpcModules(data: ByteArray): Map<String, Boolean> {
        val modules = Global.objectMapper.readValue(data, object : TypeReference<HashMap<String, String>>() {})
        return DefaultEthereumMethods(upstream.getChain())
            .getAllMethods()
            .associateWith { method ->
                if (config.methodGroups?.enabled?.any { group -> method.startsWith(group) } == true) {
                    return@associateWith true
                }
                if (config.methodGroups?.disabled?.any { group -> method.startsWith(group) } == true) {
                    return@associateWith false
                }
                modules.any { (module, _) -> method.startsWith(module) }
            }
    }
}
