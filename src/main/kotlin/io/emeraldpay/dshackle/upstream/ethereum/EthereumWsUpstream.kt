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
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumWsIngressSubscription
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcSwitchClient
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcWsClient
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable

class EthereumWsUpstream(
    id: String,
    val chain: Chain,
    forkWatch: ForkWatch,
    httpConnection: JsonRpcReader,
    ethereumWsFactory: EthereumWsFactory,
    options: UpstreamsConfig.Options,
    role: UpstreamsConfig.UpstreamRole,
    node: QuorumForLabels.QuorumItem,
    targets: CallMethods
) : EthereumUpstream(id, forkWatch, options, role, targets, node), Upstream, Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumWsUpstream::class.java)
    }

    private val head: EthereumWsHead
    private val connection: WsConnectionImpl
    private val api: JsonRpcReader
    private val subscriptions: EthereumIngressSubscription

    private var validatorSubscription: Disposable? = null

    init {
        connection = ethereumWsFactory.create(this)
        val wsSubscriptions = WsSubscriptionsImpl(connection)
        // Sometimes the server may close the WebSocket connection during the execution of a call, for example if the response
        // is too large for WebSockets Frame (and Geth is unable to split messages into separate frames)
        // In this case the failed request must be rerouted to the HTTP connection, because otherwise it would always fail
        api = JsonRpcSwitchClient(
            JsonRpcWsClient(connection), httpConnection
        )

        head = EthereumWsHead(getApi(), wsSubscriptions)
        subscriptions = EthereumWsIngressSubscription(wsSubscriptions)
    }

    override fun getHead(): Head {
        return head
    }

    override fun getApi(): JsonRpcReader {
        return api
    }

    override fun isGrpc(): Boolean {
        return false
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return subscriptions
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun start() {
        super.start()
        connection.connect()
        head.start()

        if (getOptions().disableValidation) {
            log.warn("Disable validation for upstream ${this.getId()}")
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            log.debug("Start validation for upstream ${this.getId()}")
            val validator = EthereumUpstreamValidator(this, getOptions())
            validatorSubscription = validator.start()
                .subscribe(this::setStatus)
        }
    }

    override fun stop() {
        super.stop()
        validatorSubscription?.dispose()
        validatorSubscription = null
        head.stop()
        connection.close()
    }

    override fun isRunning(): Boolean {
        return super.isRunning() || head.isRunning
    }
}
