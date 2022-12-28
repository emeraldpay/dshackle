package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcHttpClient
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer

open class HttpRpcFactory(
    private val url: String,
    private val basicAuth: AuthConfig.ClientBasicAuth?,
    private val tls: ByteArray?
) : HttpFactory {
    override fun create(id: String?, chain: Chain): JsonRpcReader {
        val metricsTags = listOf(
            // "unknown" is not supposed to happen
            Tag.of("upstream", id ?: "unknown"),
            // UNSPECIFIED shouldn't happen too
            Tag.of("chain", chain.chainCode)
        )
        val metrics = RpcMetrics(
            Timer.builder("upstream.rpc.conn")
                .description("Request time through a HTTP JSON RPC connection")
                .tags(metricsTags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry),
            Counter.builder("upstream.rpc.fail")
                .description("Number of failures of HTTP JSON RPC requests")
                .tags(metricsTags)
                .register(Metrics.globalRegistry)
        )
        return JsonRpcHttpClient(
            url,
            metrics,
            basicAuth,
            tls
        )
    }
}
