package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig.Labels
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.LabelsDetectorBuilder
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.UpstreamValidatorBuilder
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

open class GenericUpstream(
    id: String,
    val chain: Chain,
    hash: Byte,
    options: ChainOptions.Options,
    role: UpstreamsConfig.UpstreamRole,
    targets: CallMethods?,
    private val node: QuorumForLabels.QuorumItem?,
    chainConfig: ChainsConfig.ChainConfig,
    connectorFactory: ConnectorFactory,
    private val eventPublisher: ApplicationEventPublisher?,
    validatorBuilder: UpstreamValidatorBuilder,
    labelsDetectorBuilder: LabelsDetectorBuilder,
    private val subscriptionTopics: (GenericUpstream) -> List<String>,
) : DefaultUpstream(id, hash, null, UpstreamAvailability.OK, options, role, targets, node, chainConfig), Lifecycle {

    private val validator: UpstreamValidator? = validatorBuilder(chain, this, getOptions(), chainConfig)
    private var validatorSubscription: Disposable? = null
    private var validationSettingsSubscription: Disposable? = null

    private val hasLiveSubscriptionHead: AtomicBoolean = AtomicBoolean(false)
    protected val connector: GenericConnector = connectorFactory.create(this, chain)
    private var livenessSubscription: Disposable? = null
    private val labelsDetector = labelsDetectorBuilder(chain, this.getIngressReader())

    override fun getHead(): Head {
        return connector.getHead()
    }

    override fun getIngressReader(): JsonRpcReader {
        return connector.getIngressReader()
    }

    override fun getLabels(): Collection<Labels> {
        return node?.let { listOf(it.labels) } ?: emptyList()
    }

    override fun getSubscriptionTopics(): List<String> {
        return subscriptionTopics(this)
    }

    // outdated, looks like applicable only for bitcoin and our ws_head trick
    override fun getCapabilities(): Set<Capability> {
        return if (hasLiveSubscriptionHead.get()) {
            setOf(Capability.RPC, Capability.BALANCE, Capability.WS_HEAD)
        } else {
            setOf(Capability.RPC, Capability.BALANCE)
        }
    }

    override fun isGrpc(): Boolean {
        // this implementation works only with statically configured upstreams
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
        log.info("Configured for ${chain.chainName}")
        connector.start()

        if (validator != null) {
            val validSettingsResult = validator.validateUpstreamSettingsOnStartup()
            when (validSettingsResult) {
                ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR -> {
                    connector.stop()
                    log.warn("Upstream ${getId()} couldn't start, invalid upstream settings")
                    return
                }
                ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR -> {
                    validateUpstreamSettings()
                }
                else -> {
                    upstreamStart()
                }
            }
        } else {
            upstreamStart()
        }
    }

    private fun validateUpstreamSettings() {
        if (validator != null) {
            validationSettingsSubscription = Flux.interval(
                Duration.ofSeconds(10),
                Duration.ofSeconds(20),
            ).flatMap {
                validator.validateUpstreamSettings()
            }.subscribe {
                when (it) {
                    ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR -> {
                        connector.stop()
                        disposeValidationSettingsSubscription()
                    }

                    ValidateUpstreamSettingsResult.UPSTREAM_VALID -> {
                        upstreamStart()
                        eventPublisher?.publishEvent(
                            UpstreamChangeEvent(
                                chain,
                                this,
                                UpstreamChangeEvent.ChangeType.ADDED,
                            ),
                        )
                        disposeValidationSettingsSubscription()
                    }

                    else -> {
                        log.warn("Continue validation of upstream ${getId()}")
                    }
                }
            }
        }
    }

    private fun detectLabels() {
        labelsDetector?.detectLabels()?.subscribe { label -> updateLabels(label) }
    }

    private fun upstreamStart() {
        if (getOptions().disableValidation) {
            log.warn("Disable validation for upstream ${this.getId()}")
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            log.debug("Start validation for upstream ${this.getId()}")
            validatorSubscription = validator?.start()
                ?.subscribe(this::setStatus)
        }
        livenessSubscription = connector.hasLiveSubscriptionHead().subscribe({
            hasLiveSubscriptionHead.set(it)
            eventPublisher?.publishEvent(UpstreamChangeEvent(chain, this, UpstreamChangeEvent.ChangeType.UPDATED))
        }, {
            log.debug("Error while checking live subscription for ${getId()}", it)
        },)
        detectLabels()
    }

    override fun stop() {
        validatorSubscription?.dispose()
        validatorSubscription = null
        livenessSubscription?.dispose()
        livenessSubscription = null
        disposeValidationSettingsSubscription()
        connector.stop()
    }

    private fun disposeValidationSettingsSubscription() {
        validationSettingsSubscription?.dispose()
        validationSettingsSubscription = null
    }

    private fun updateLabels(label: Pair<String, String>) {
        log.info("Detected label ${label.first} with value ${label.second} for upstream ${getId()}")
        node?.labels?.let { labels ->
            labels[label.first] = label.second
        }
    }

    fun getIngressSubscription(): IngressSubscription {
        return connector.getIngressSubscription()
    }

    override fun isRunning() = connector.isRunning()
}
