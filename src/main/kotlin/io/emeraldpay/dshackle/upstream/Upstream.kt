package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import reactor.core.publisher.Flux

interface Upstream {
    fun isAvailable(matcher: Selector.Matcher): Boolean
    fun getStatus(): UpstreamAvailability
    fun observeStatus(): Flux<UpstreamAvailability>
    fun getHead(): EthereumHead
    fun getApi(matcher: Selector.Matcher): DirectEthereumApi
//    fun getCache(): CachingEthereumApi
    fun getOptions(): UpstreamsConfig.Options
    fun getSupportedTargets(): Set<String>
    fun setLag(lag: Long)
    fun getLag(): Long
}