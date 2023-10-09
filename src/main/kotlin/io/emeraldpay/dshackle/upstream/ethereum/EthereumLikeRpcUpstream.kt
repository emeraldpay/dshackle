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
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator.ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator.ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR
import io.emeraldpay.dshackle.upstream.ethereum.EthereumUpstreamValidator.ValidateUpstreamSettingsResult.UPSTREAM_VALID
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.EthereumLabelsDetector
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

open class EthereumLikeRpcUpstream(
    id: String,
    hash: Byte,
    val chain: Chain,
    options: ChainOptions.Options,
    role: UpstreamsConfig.UpstreamRole,
    targets: CallMethods?,
    private val node: QuorumForLabels.QuorumItem?,
    connectorFactory: ConnectorFactory,
    chainConfig: ChainsConfig.ChainConfig,
    skipEnhance: Boolean,
    private val eventPublisher: ApplicationEventPublisher?,
) : EthereumLikeUpstream(id, hash, options, role, targets, node, chainConfig), Lifecycle, Upstream, CachesEnabled {
    private val validator: EthereumUpstreamValidator = EthereumUpstreamValidator(chain, this, getOptions(), chainConfig.callLimitContract)
    protected val connector: GenericConnector = connectorFactory.create(this, chain, skipEnhance)
    private val labelsDetector = EthereumLabelsDetector(this.getIngressReader(), chain)
    private var hasLiveSubscriptionHead: AtomicBoolean = AtomicBoolean(false)

    private var validatorSubscription: Disposable? = null
    private var livenessSubscription: Disposable? = null
    private var validationSettingsSubscription: Disposable? = null

    override fun getCapabilities(): Set<Capability> {
        return if (hasLiveSubscriptionHead.get()) {
            setOf(Capability.RPC, Capability.BALANCE, Capability.WS_HEAD)
        } else {
            setOf(Capability.RPC, Capability.BALANCE)
        }
    }

    override fun setCaches(caches: Caches) {
        if (connector is CachesEnabled) {
            connector.setCaches(caches)
        }
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")
        connector.start()
        val validSettingsResult = validator.validateUpstreamSettingsOnStartup()
        when (validSettingsResult) {
            UPSTREAM_FATAL_SETTINGS_ERROR -> {
                connector.stop()
                log.warn("Upstream ${getId()} couldn't start, invalid upstream settings")
                return
            }
            UPSTREAM_SETTINGS_ERROR -> {
                validateUpstreamSettings()
            }
            else -> {
                upstreamStart()
                labelsDetector.detectLabels()
                    .toStream()
                    .forEach { updateLabels(it) }
            }
        }
    }

    private fun validateUpstreamSettings() {
        validationSettingsSubscription = Flux.interval(
            Duration.ofSeconds(10),
            Duration.ofSeconds(20),
        ).flatMap {
            validator.validateUpstreamSettings()
        }.subscribe {
            when (it) {
                UPSTREAM_FATAL_SETTINGS_ERROR -> {
                    connector.stop()
                    log.warn("Upstream ${getId()} couldn't start, invalid upstream settings")
                    disposeValidationSettingsSubscription()
                }
                UPSTREAM_VALID -> {
                    upstreamStart()
                    labelsDetector.detectLabels()
                        .subscribe { label -> updateLabels(label) }
                    eventPublisher?.publishEvent(UpstreamChangeEvent(chain, this, UpstreamChangeEvent.ChangeType.ADDED))
                    disposeValidationSettingsSubscription()
                }
                else -> {
                    log.warn("Continue validation of upstream ${getId()}")
                }
            }
        }
    }

    private fun upstreamStart() {
        if (getOptions().disableValidation) {
            log.warn("Disable validation for upstream ${this.getId()}")
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            log.debug("Start validation for upstream ${this.getId()}")
            validatorSubscription = validator.start()
                .subscribe(this::setStatus)
        }
        livenessSubscription = connector.hasLiveSubscriptionHead().subscribe({
            hasLiveSubscriptionHead.set(it)
            eventPublisher?.publishEvent(UpstreamChangeEvent(chain, this, UpstreamChangeEvent.ChangeType.UPDATED))
        }, {
            log.debug("Error while checking live subscription for ${getId()}", it)
        },)
    }

    private fun updateLabels(label: Pair<String, String>) {
        log.info("Detected label ${label.first} with value ${label.second} for upstream ${getId()}")
        node?.labels?.let { labels ->
            labels[label.first] = label.second
        }
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return connector.getIngressSubscription()
    }

    override fun getSubscriptionTopics(): List<String> {
        val subs = if (getCapabilities().contains(Capability.WS_HEAD)) {
            listOf(EthereumEgressSubscription.METHOD_NEW_HEADS, EthereumEgressSubscription.METHOD_LOGS)
        } else {
            listOf()
        }
        return getIngressSubscription().getAvailableTopics().plus(subs).toSet().toList()
    }

    override fun getHead(): Head {
        return connector.getHead()
    }

    override fun stop() {
        validatorSubscription?.dispose()
        validatorSubscription = null
        livenessSubscription?.dispose()
        livenessSubscription = null
        disposeValidationSettingsSubscription()
        connector.stop()
    }

    override fun isRunning(): Boolean {
        return connector.isRunning() && validationSettingsSubscription == null
    }

    override fun getIngressReader(): JsonRpcReader {
        return connector.getIngressReader()
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

    private fun disposeValidationSettingsSubscription() {
        validationSettingsSubscription?.dispose()
        validationSettingsSubscription = null
    }
}
