/**
 * Copyright (c) 2020 EmeraldPay, Inc
 * Copyright (c) 2019 ETCDEV GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import java.net.URI
import java.util.function.Consumer

open class EthereumWsFactory(
    private val id: String,
    private val chain: Chain,
    private val uri: URI,
    private val origin: URI,
) {

    var basicAuth: AuthConfig.ClientBasicAuth? = null
    var config: UpstreamsConfig.WsEndpoint? = null

    // metrics are shared between all connections to the same WS
    private val metrics: RpcMetrics = run {
        val metricsTags = listOf(
            Tag.of("upstream", id),
            // UNSPECIFIED shouldn't happen too
            Tag.of("chain", chain.chainCode)
        )

        RpcMetrics(
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
    }

    open fun create(onConnectionChange: Consumer<WsConnection.ConnectionStatus>?): WsConnection {
        return WsConnectionImpl(uri, origin, basicAuth, metrics).also { ws ->
            ws.onConnectionChange(onConnectionChange)
            config?.frameSize?.let {
                ws.frameSize = it
            }
            config?.msgSize?.let {
                ws.msgSizeLimit = it
            }
        }
    }
}
