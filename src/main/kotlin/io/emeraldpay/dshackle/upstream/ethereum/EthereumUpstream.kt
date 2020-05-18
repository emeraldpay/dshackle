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

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.time.Duration

open class EthereumUpstream(
        id: String,
        val chain: Chain,
        private val directReader: Reader<JsonRpcRequest, JsonRpcResponse>,
        private val ethereumWsFactory: EthereumWsFactory? = null,
        options: UpstreamsConfig.Options,
        val node: QuorumForLabels.QuorumItem,
        targets: CallMethods
) : DefaultUpstream(id, options, targets), Upstream, CachesEnabled, Lifecycle {

    constructor(id: String, chain: Chain, api: Reader<JsonRpcRequest, JsonRpcResponse>) : this(id, chain, api, null,
            UpstreamsConfig.Options.getDefaults(), QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels()),
            DirectCallMethods())


    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    private val head: Head = this.createHead()
    private var validatorSubscription: Disposable? = null

    override fun setCaches(caches: Caches) {
        if (head is CachesEnabled) {
            head.setCaches(caches)
        }
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")

        if (getOptions().disableValidation != null && getOptions().disableValidation!!) {
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            val validator = EthereumUpstreamValidator(this, getOptions())
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

    open fun createHead(): Head {
        return if (ethereumWsFactory != null) {
            val ws = ethereumWsFactory.create(this).apply {
                connect()
            }
            val wsHead = EthereumWsHead(ws).apply {
                start()
            }
            // receive bew blocks through WebSockets, but also periodically verify with RPC in case if WS failed
            val rpcHead = EthereumRpcHead(getApi(), Duration.ofSeconds(60)).apply {
                start()
            }
            MergedHead(listOf(rpcHead, wsHead)).apply {
                start()
            }
        } else {
            log.warn("Setting up upstream ${this.getId()} with RPC-only access, less effective than WS+RPC")
            EthereumRpcHead(getApi()).apply {
                start()
            }
        }
    }

    override fun getHead(): Head {
        return head
    }

    override fun getApi(): Reader<JsonRpcRequest, JsonRpcResponse> {
        return directReader
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return listOf(node.labels)
    }

    @Suppress("unchecked")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

}