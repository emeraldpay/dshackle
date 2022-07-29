package io.emeraldpay.dshackle.upstream.ethereum.connectors

import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsFactory
import io.emeraldpay.dshackle.upstream.ethereum.EthereumWsHead
import io.emeraldpay.dshackle.upstream.ethereum.WsConnection
import io.emeraldpay.dshackle.upstream.forkchoice.ForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer

class EthereumWsConnector(
    wsFactory: EthereumWsFactory,
    upstream: DefaultUpstream,
    validator: EthereumUpstreamValidator,
    chain: Chain,
    forkChoice: ForkChoice
) : EthereumConnector {
    private val conn: WsConnection
    private val api: Reader<JsonRpcRequest, JsonRpcResponse>
    private val head: EthereumWsHead

    init {
        val metricsTags = listOf(
            Tag.of("upstream", upstream.getId()),
            // UNSPECIFIED shouldn't happen too
            Tag.of("chain", chain.chainCode)
        )
        val metrics = RpcMetrics(
            Timer.builder("upstream.ws.conn")
                .description("Request time through a WebSocket JSON RPC connection")
                .tags(metricsTags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry),
            Counter.builder("upstream.ws.fail")
                .description("Number of failures of WebSocket JSON RPC requests")
                .tags(metricsTags)
                .register(Metrics.globalRegistry)
        )

        conn = wsFactory.create(upstream, validator, metrics)
        head = EthereumWsHead(conn, forkChoice)
        api = JsonRpcWsClient(conn)
    }

    override fun start() {
        conn.connect()
        head.start()
    }

    override fun isRunning(): Boolean {
       return head.isRunning
    }

    override fun stop() {
        conn.close()
        head.stop()
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return api
    }

    override fun getHead(): Head {
        return head
    }
}