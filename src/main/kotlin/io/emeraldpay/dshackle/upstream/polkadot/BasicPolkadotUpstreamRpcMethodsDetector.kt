package io.emeraldpay.dshackle.upstream.polkadot

import com.fasterxml.jackson.core.type.TypeReference
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.ChainResponse
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamRpcMethodsDetector
import io.emeraldpay.dshackle.upstream.rpcclient.CallParams
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import reactor.core.publisher.Mono

class BasicPolkadotUpstreamRpcMethodsDetector(
    private val upstream: Upstream,
) : UpstreamRpcMethodsDetector(upstream) {
    override fun detectByMagicMethod(): Mono<Map<String, Boolean>> =
        upstream
            .getIngressReader()
            .read(ChainRequest("rpc_methods", ListParams()))
            .flatMap(ChainResponse::requireResult)
            .map {
                Global.objectMapper
                    .readValue(it, object : TypeReference<HashMap<String, List<String>>>() {})
                    .getOrDefault("methods", emptyList())
                    .associateWith { true }
            }.onErrorResume {
                log.warn(
                    "Can't detect rpc method rpc_methods of upstream ${upstream.getId()}, reason - {}",
                    it.message,
                )
                Mono.empty()
            }

    override fun rpcMethods(): Set<Pair<String, CallParams>> = emptySet()
}
