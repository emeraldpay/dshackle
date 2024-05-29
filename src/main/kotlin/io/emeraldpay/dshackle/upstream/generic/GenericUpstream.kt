package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig.Labels
import io.emeraldpay.dshackle.foundation.ChainOptions
import io.emeraldpay.dshackle.reader.ChainReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent
import io.emeraldpay.dshackle.startup.UpstreamChangeEvent.ChangeType.UPDATED
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.DefaultUpstream
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.UNKNOWN_CLIENT_VERSION
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetectorBuilder
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.UpstreamValidatorBuilder
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundServiceBuilder
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

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
    validatorBuilder: UpstreamValidatorBuilder,
    upstreamSettingsDetectorBuilder: UpstreamSettingsDetectorBuilder,
    lowerBoundServiceBuilder: LowerBoundServiceBuilder,
) : DefaultUpstream(id, hash, null, UpstreamAvailability.OK, options, role, targets, node, chainConfig), Lifecycle {

    private val validator: UpstreamValidator? = validatorBuilder(chain, this, getOptions(), chainConfig)
    private var validatorSubscription: Disposable? = null
    private var validationSettingsSubscription: Disposable? = null
    private var lowerBlockDetectorSubscription: Disposable? = null

    private val hasLiveSubscriptionHead: AtomicBoolean = AtomicBoolean(false)
    protected val connector: GenericConnector = connectorFactory.create(this, chain)
    private var livenessSubscription: Disposable? = null
    private val settingsDetector = upstreamSettingsDetectorBuilder(chain, this)

    private val lowerBoundService = lowerBoundServiceBuilder(chain, this)

    private val started = AtomicBoolean(false)
    private val isUpstreamValid = AtomicBoolean(false)
    private val clientVersion = AtomicReference(UNKNOWN_CLIENT_VERSION)

    override fun getHead(): Head {
        return connector.getHead()
    }

    override fun getIngressReader(): ChainReader {
        return connector.getIngressReader()
    }

    override fun getLabels(): Collection<Labels> {
        return node?.let { listOf(it.labels) } ?: emptyList()
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

    override fun getLowerBounds(): Collection<LowerBoundData> {
        return lowerBoundService.getLowerBounds()
    }

    override fun getLowerBound(lowerBoundType: LowerBoundType): LowerBoundData? {
        return lowerBoundService.getLowerBound(lowerBoundType)
    }

    override fun getUpstreamSettingsData(): Upstream.UpstreamSettingsData? {
        return Upstream.UpstreamSettingsData(
            nodeId(),
            getId(),
            clientVersion.get(),
        )
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
                    log.warn("Upstream ${getId()} couldn't start, invalid upstream settings")
                    connector.stop()
                    return
                }
                ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR -> {
                    log.warn("Non fatal upstream settings error, continue validation...")
                    connector.getHead().stop()
                }
                ValidateUpstreamSettingsResult.UPSTREAM_VALID -> {
                    isUpstreamValid.set(true)
                    upstreamStart()
                }
            }
            validateUpstreamSettings()
        } else {
            isUpstreamValid.set(true)
            upstreamStart()
        }

        started.set(true)
    }

    private fun validateUpstreamSettings() {
        if (validator != null) {
            validationSettingsSubscription = Flux.interval(
                Duration.ofSeconds(20),
            ).flatMap {
                validator.validateUpstreamSettings()
            }
                .distinctUntilChanged()
                .subscribe {
                    when (it) {
                        ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR -> {
                            if (isUpstreamValid.get()) {
                                log.warn("There is a fatal error after upstream settings validation, removing ${getId()}...")
                                partialStop()
                                sendUpstreamStateEvent(UpstreamChangeEvent.ChangeType.FATAL_SETTINGS_ERROR_REMOVED)
                            }
                            isUpstreamValid.set(false)
                        }

                        ValidateUpstreamSettingsResult.UPSTREAM_VALID -> {
                            if (!isUpstreamValid.get()) {
                                log.warn("Upstream ${getId()} is now valid, adding to the multistream...")
                                upstreamStart()
                                sendUpstreamStateEvent(UpstreamChangeEvent.ChangeType.ADDED)
                            }
                            isUpstreamValid.set(true)
                        }

                        else -> {
                            log.warn("Continue validation of upstream ${getId()}")
                        }
                    }
                }
        }
    }

    private fun detectSettings() {
        settingsDetector?.detectLabels()?.subscribe { label -> updateLabels(label) }

        settingsDetector?.detectClientVersion()
            ?.subscribe {
                log.info("Detected node version $it for upstream ${getId()}")
                clientVersion.set(it)
            }
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
            sendUpstreamStateEvent(UPDATED)
        }, {
            log.debug("Error while checking live subscription for ${getId()}", it)
        },)
        detectSettings()

        detectLowerBlock()
    }

    override fun stop() {
        partialStop()
        validationSettingsSubscription?.dispose()
        validationSettingsSubscription = null
        connector.stop()
        started.set(false)
    }

    private fun partialStop() {
        validatorSubscription?.dispose()
        validatorSubscription = null
        livenessSubscription?.dispose()
        livenessSubscription = null
        lowerBlockDetectorSubscription?.dispose()
        lowerBlockDetectorSubscription = null
        connector.getHead().stop()
    }

    private fun updateLabels(label: Pair<String, String>) {
        log.info("Detected label ${label.first} with value ${label.second} for upstream ${getId()}")
        node?.labels?.let { labels ->
            labels[label.first] = label.second
        }
    }

    private fun detectLowerBlock() {
        lowerBlockDetectorSubscription = lowerBoundService.detectLowerBounds()
            .subscribe {
                sendUpstreamStateEvent(UPDATED)
            }
    }

    fun getIngressSubscription(): IngressSubscription {
        return connector.getIngressSubscription()
    }

    override fun isRunning() = connector.isRunning() || started.get()

    override fun updateLowerBound(lowerBound: Long, type: LowerBoundType) {
        lowerBoundService.updateLowerBound(lowerBound, type)
    }

    fun isValid(): Boolean = isUpstreamValid.get()

    private fun sendUpstreamStateEvent(eventType: UpstreamChangeEvent.ChangeType) {
        stateEventStream.emitNext(
            UpstreamChangeEvent(chain, this, eventType),
        ) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
    }
}
