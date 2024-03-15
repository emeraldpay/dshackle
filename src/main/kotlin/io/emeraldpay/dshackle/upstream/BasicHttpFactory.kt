package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.ApiType
import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.upstream.restclient.RestHttpReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcHttpReader
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer

class BasicHttpFactory(
    private val url: String,
    private val basicAuth: AuthConfig.ClientBasicAuth?,
    private val tls: ByteArray?,
) : HttpFactory {
    override fun create(id: String?, chain: Chain): HttpReader {
        val metricsTags = listOf(
            // "unknown" is not supposed to happen
            Tag.of("upstream", id ?: "unknown"),
            // UNSPECIFIED shouldn't happen too
            Tag.of("chain", chain.chainCode),
        )
        val metrics = RequestMetrics(
            Timer.builder("upstream.rpc.conn")
                .description("Request time through a HTTP JSON RPC connection")
                .tags(metricsTags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry),
            Counter.builder("upstream.rpc.fail")
                .description("Number of failures of HTTP JSON RPC requests")
                .tags(metricsTags)
                .register(Metrics.globalRegistry),
        )

        if (chain.type.apiType == ApiType.REST) {
            return RestHttpReader(url, metrics, basicAuth, tls)
        }
        return JsonRpcHttpReader(url, metrics, basicAuth, tls)
    }
}
