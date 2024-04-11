/**
 * Copyright (c) 2020 EmeraldPay, Inc
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
package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.HttpReader
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import reactor.core.Disposable

open class BitcoinRpcUpstream(
    id: String,
    chain: Chain,
    private val directApi: HttpReader,
    private val head: Head,
    options: ChainOptions.Options,
    role: UpstreamsConfig.UpstreamRole,
    node: QuorumForLabels.QuorumItem,
    callMethods: CallMethods,
    esploraClient: EsploraClient? = null,
    chainConfig: ChainsConfig.ChainConfig,
) : BitcoinUpstream(id, chain, options, role, callMethods, node, esploraClient, chainConfig), Lifecycle {

    private var validatorSubscription: Disposable? = null

    private val capabilities = if (options.providesBalance == true) {
        setOf(Capability.RPC, Capability.BALANCE)
    } else {
        setOf(Capability.RPC)
    }

    override fun getHead(): Head {
        return head
    }

    override fun getIngressReader(): ChainReader {
        return directApi
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return listOf(UpstreamsConfig.Labels())
    }

    override fun getCapabilities(): Set<Capability> {
        return capabilities
    }

    override fun isGrpc(): Boolean {
        return false
    }

    override fun getLowerBounds(): Collection<LowerBoundData> {
        return emptyList()
    }

    override fun getUpstreamSettingsData(): Upstream.UpstreamSettingsData? {
        return null
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }

    override fun isRunning(): Boolean {
        var runningAny = validatorSubscription != null
        if (head is Lifecycle) {
            runningAny = runningAny || head.isRunning()
        }
        return runningAny
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")
        if (head is Lifecycle) {
            if (!head.isRunning()) {
                head.start()
            }
        }

        validatorSubscription?.dispose()

        if (getOptions().disableValidation) {
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            val validator = BitcoinUpstreamValidator(directApi, getOptions())
            validatorSubscription = validator.start()
                .subscribe(this::setStatus)
        }
    }

    override fun stop() {
        if (head is Lifecycle) {
            head.stop()
        }
        validatorSubscription?.dispose()
        directApi.onStop()
    }
}
