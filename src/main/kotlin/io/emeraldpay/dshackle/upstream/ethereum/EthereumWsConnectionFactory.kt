package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import reactor.core.scheduler.Scheduler
import java.net.URI

open class EthereumWsConnectionFactory(
    private val id: String,
    private val chain: Chain,
    private val uri: URI,
    private val origin: URI,
    private val scheduler: Scheduler,
) {

    var basicAuth: AuthConfig.ClientBasicAuth? = null
    var config: UpstreamsConfig.WsEndpoint? = null

    private fun metrics(connIndex: Int): RpcMetrics {
        val metricsTags = listOf(
            Tag.of("index", connIndex.toString()),
            Tag.of("upstream", id),
            // UNSPECIFIED shouldn't happen too
            Tag.of("chain", chain.chainCode),
        )

        return RpcMetrics(
            Timer.builder("upstream.ws.conn")
                .description("Request time through a WebSocket JSON RPC connection")
                .tags(metricsTags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry),
            Counter.builder("upstream.ws.fail")
                .description("Number of failures of WebSocket JSON RPC requests")
                .tags(metricsTags)
                .register(Metrics.globalRegistry),
        )
    }

    open fun createWsConnection(connIndex: Int = 0, onDisconnect: () -> Unit): WsConnection =
        WsConnectionImpl(uri, origin, basicAuth, metrics(connIndex), onDisconnect, scheduler).also { ws ->
            config?.frameSize?.let {
                ws.frameSize = it
            }
            config?.msgSize?.let {
                ws.msgSizeLimit = it
            }
        }
}
