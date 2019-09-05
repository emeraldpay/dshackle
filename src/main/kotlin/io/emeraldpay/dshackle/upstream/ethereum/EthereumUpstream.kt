/**
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

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import java.time.Duration

open class EthereumUpstream(
        private val id: String,
        val chain: Chain,
        private val api: DirectEthereumApi,
        private val ethereumWs: EthereumWs? = null,
        private val options: UpstreamsConfig.Options,
        val node: NodeDetailsList.NodeDetails,
        private val targets: CallMethods
): DefaultUpstream(), Lifecycle {

    constructor(id: String, chain: Chain, api: DirectEthereumApi): this(id, chain, api, null,
            UpstreamsConfig.Options.getDefaults(), NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels()),
            DirectCallMethods())


    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    private val head: EthereumHead = this.createHead()
    private var validatorSubscription: Disposable? = null

    init {
        api.upstream = this
    }

    override fun getId(): String {
        return id
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")

        if (options.disableValidation != null && options.disableValidation!!) {
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            val validator = UpstreamValidator(this, options)
            validatorSubscription = validator.start()
                    .subscribe(this::setStatus)
        }
    }

    override fun isRunning(): Boolean {
        return true
    }

    override fun stop() {
        validatorSubscription?.dispose()
        validatorSubscription = null
        if (head is Lifecycle) {
            head.stop()
        }
    }

    open fun createHead(): EthereumHead {
        return if (ethereumWs != null) {
            val ws = EthereumWsHead(ethereumWs).apply {
                this.start()
            }
            val rpc = EthereumRpcHead(api, Duration.ofSeconds(20)).apply {
                this.start()
            }
            EthereumHeadMerge(listOf(rpc.getFlux(), ws.getFlux())).apply {
                this.start()
            }
        } else {
            EthereumRpcHead(api).apply {
                this.start()
            }
        }
    }

    override fun isAvailable(): Boolean {
        return getStatus() == UpstreamAvailability.OK
    }

    override fun getHead(): EthereumHead {
        return head
    }

    override fun getApi(matcher: Selector.Matcher): DirectEthereumApi {
        return api
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return listOf(node.labels)
    }

    override fun getMethods(): CallMethods {
        return targets
    }

}