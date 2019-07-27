package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import reactor.core.publisher.Flux

interface Upstream {
    fun isAvailable(): Boolean
    fun getStatus(): UpstreamAvailability
    fun observeStatus(): Flux<UpstreamAvailability>
    fun getHead(): EthereumHead
    fun getApi(): EthereumApi
    fun getOptions(): UpstreamsConfig.Options
}