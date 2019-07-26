package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig

interface Upstream {
    fun isAvailable(): Boolean
    fun getStatus(): UpstreamAvailability
    fun getHead(): EthereumHead
    fun getApi(): EthereumApi
    fun getOptions(): UpstreamsConfig.Options
}