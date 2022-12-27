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

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.ethereum.connectors.EthereumConnector
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable

open class EthereumRpcUpstream(
    id: String,
    hash: Byte,
    val chain: Chain,
    options: UpstreamsConfig.Options,
    role: UpstreamsConfig.UpstreamRole,
    targets: CallMethods?,
    private val node: QuorumForLabels.QuorumItem?,
    connectorFactory: ConnectorFactory
) : EthereumUpstream(id, hash, options, role, targets, node), Lifecycle, Upstream, CachesEnabled {
    private val log = LoggerFactory.getLogger(EthereumRpcUpstream::class.java)
    private val validator: EthereumUpstreamValidator = EthereumUpstreamValidator(this, getOptions())
    private val connector: EthereumConnector = connectorFactory.create(this, validator, chain)

    private var validatorSubscription: Disposable? = null

    override fun setCaches(caches: Caches) {
        if (connector is CachesEnabled) {
            connector.setCaches(caches)
        }
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")
        connector.start()
        if (getOptions().disableValidation != null && getOptions().disableValidation!!) {
            log.warn("Disable validation for upstream ${this.getId()}")
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            log.debug("Start validation for upstream ${this.getId()}")
            validatorSubscription = validator.start()
                .subscribe(this::setStatus)
        }
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return connector.getIngressSubscription()
    }

    override fun getHead(): Head {
        return connector.getHead()
    }

    override fun stop() {
        validatorSubscription?.dispose()
        validatorSubscription = null
        connector.stop()
    }

    override fun isRunning(): Boolean {
        return connector.isRunning()
    }

    override fun getApi(): JsonRpcReader {
        return connector.getApi()
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
}
