/**
 * Copyright (c) 2021 EmeraldPay, Inc
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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import io.emeraldpay.dshackle.upstream.rpcclient.RpcMetrics
import io.emeraldpay.grpc.Chain
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable

class EthereumWsUpstream(
        id: String,
        val chain: Chain,
        ethereumWsFactory: EthereumWsFactory,
        options: UpstreamsConfig.Options,
        role: UpstreamsConfig.UpstreamRole,
        node: QuorumForLabels.QuorumItem,
        targets: CallMethods
) : EthereumUpstream(id, options, role, targets, node), Upstream, Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumWsUpstream::class.java)
    }

    private val head: EthereumWsHead
    private val connection: EthereumWsFactory.EthereumWs
    private val api: JsonRpcWsClient

    private var validatorSubscription: Disposable? = null
    private val validator: EthereumUpstreamValidator

    init {
        val metricsTags = listOf(
                Tag.of("upstream", id),
                // UNSPECIFIED shouldn't happen too
                Tag.of("chain", chain.chainCode)
        )
        val metrics = RpcMetrics(
                Timer.builder("upstream.ws.conn")
                        .description("Request time through a WebSocket JSON RPC connection")
                        .tags(metricsTags)
                        .publishPercentileHistogram()
                        .register(Metrics.globalRegistry),
                Counter.builder("upstream.ws.err")
                        .description("Errors received on request through WebSocket JSON RPC connection")
                        .tags(metricsTags)
                        .register(Metrics.globalRegistry)
        )

        validator = EthereumUpstreamValidator(this, getOptions())

        connection = ethereumWsFactory.create(this, validator, metrics)
        head = EthereumWsHead(connection)
        api = JsonRpcWsClient(connection)
    }

    override fun getHead(): Head {
        return head
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return api
    }

    override fun isGrpc(): Boolean {
        return false
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun start() {
        connection.connect()
        head.start()

        log.debug("Start validation for upstream ${this.getId()}")
        validatorSubscription = validator.start()
                .subscribe(this::setStatus)
    }

    override fun stop() {
        validatorSubscription?.dispose()
        validatorSubscription = null
        head.stop()
        connection.close()
    }

    override fun isRunning(): Boolean {
        return head.isRunning
    }
}