package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.config.ChainsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.config.UpstreamsConfig.Labels
import io.emeraldpay.dshackle.config.hot.CompatibleVersionsRules
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
import io.emeraldpay.dshackle.upstream.UpstreamRpcModulesDetector
import io.emeraldpay.dshackle.upstream.UpstreamRpcModulesDetectorBuilder
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetectorBuilder
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.UpstreamValidatorBuilder
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundServiceBuilder
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

open class GenericUpstream(
    id: String,
    chain: Chain,
    hash: Byte,
    options: ChainOptions.Options,
    role: UpstreamsConfig.UpstreamRole,
    targets: CallMethods?,
    private val node: QuorumForLabels.QuorumItem?,
    private val chainConfig: ChainsConfig.ChainConfig,
    connectorFactory: ConnectorFactory,
    validatorBuilder: UpstreamValidatorBuilder,
    upstreamSettingsDetectorBuilder: UpstreamSettingsDetectorBuilder,
    lowerBoundServiceBuilder: LowerBoundServiceBuilder,
    finalizationDetectorBuilder: FinalizationDetectorBuilder,
    versionRules: Supplier<CompatibleVersionsRules?>,
) : DefaultUpstream(id, hash, null, UpstreamAvailability.OK, options, role, targets, node, chainConfig, chain), Lifecycle {

    constructor(
        config: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        hash: Byte,
        options: ChainOptions.Options,
        node: QuorumForLabels.QuorumItem?,
        chainConfig: ChainsConfig.ChainConfig,
        connectorFactory: ConnectorFactory,
        validatorBuilder: UpstreamValidatorBuilder,
        upstreamSettingsDetectorBuilder: UpstreamSettingsDetectorBuilder,
        upstreamRpcModulesDetectorBuilder: UpstreamRpcModulesDetectorBuilder,
        buildMethods: (UpstreamsConfig.Upstream<*>, Chain) -> CallMethods,
        lowerBoundServiceBuilder: LowerBoundServiceBuilder,
        finalizationDetectorBuilder: FinalizationDetectorBuilder,
        versionRules: Supplier<CompatibleVersionsRules?>,
    ) : this(config.id!!, chain, hash, options, config.role, buildMethods(config, chain), node, chainConfig, connectorFactory, validatorBuilder, upstreamSettingsDetectorBuilder, lowerBoundServiceBuilder, finalizationDetectorBuilder, versionRules) {
        rpcModulesDetector = upstreamRpcModulesDetectorBuilder(this)
        detectRpcModules(config, buildMethods)
    }

    private val validator: UpstreamValidator? = validatorBuilder(chain, this, getOptions(), chainConfig, versionRules)
    private var validatorSubscription: Disposable? = null
    private var validationSettingsSubscription: Disposable? = null
    private var lowerBlockDetectorSubscription: Disposable? = null

    private val hasLiveSubscriptionHead: AtomicBoolean = AtomicBoolean(false)
    protected val connector: GenericConnector = connectorFactory.create(this, chain)
    private var livenessSubscription: Disposable? = null
    private val settingsDetector = upstreamSettingsDetectorBuilder(chain, this)
    private var rpcModulesDetector: UpstreamRpcModulesDetector? = null

    private val lowerBoundService = lowerBoundServiceBuilder(chain, this)

    private val started = AtomicBoolean(false)
    private val isUpstreamValid = AtomicBoolean(false)
    private val clientVersion = AtomicReference(UNKNOWN_CLIENT_VERSION)

    private val finalizationDetector = finalizationDetectorBuilder()
    private var finalizationDetectorSubscription: Disposable? = null

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
        log.info("Configured for ${getChain().chainName}")
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
        Flux.interval(
            Duration.ZERO,
            Duration.ofSeconds(getOptions().validationInterval.toLong() * 5),
        ).flatMap {
            Flux.merge(
                settingsDetector?.detectLabels()
                    ?.doOnNext { label ->
                        updateLabels(label)
                        sendUpstreamStateEvent(UPDATED)
                    },
                settingsDetector?.detectClientVersion()
                    ?.doOnNext {
                        log.info("Detected node version $it for upstream ${getId()}")
                        clientVersion.set(it)
                    },
            )
        }.subscribe()
    }

    private fun detectRpcModules(config: UpstreamsConfig.Upstream<*>, buildMethods: (UpstreamsConfig.Upstream<*>, Chain) -> CallMethods) {
        rpcModulesDetector?.detectRpcModules()

        val rpcDetector = rpcModulesDetector?.detectRpcModules()?.block() ?: HashMap<String, String>()
        log.info("Upstream rpc detector for  ${getId()} returned  $rpcDetector ")
        if (rpcDetector.size != 0) {
            var changed = false
            for ((group, _) in rpcDetector) {
                if (group == "trace" || group == "debug" || group == "filter") {
                    if (config.methodGroups == null) {
                        config.methodGroups = UpstreamsConfig.MethodGroups(setOf("filter"), setOf())
                    } else {
                        val disabled = config.methodGroups!!.disabled
                        val enabled = config.methodGroups!!.enabled
                        if (!disabled.contains(group) && !enabled.contains(group)) {
                            config.methodGroups!!.enabled = enabled.plus(group)
                            changed = true
                        }
                    }
                }
            }
            if (changed) updateMethods(buildMethods(config, getChain()))
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

        detectFinalization()
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
        finalizationDetectorSubscription?.dispose()
        finalizationDetectorSubscription = null
        connector.getHead().stop()
    }

    private fun updateLabels(label: Pair<String, String>) {
        log.info("Detected label ${label.first} with value ${label.second} for upstream ${getId()}")
        node?.labels?.let { labels ->
            labels[label.first] = label.second
        }
    }

    override fun getFinalizations(): Collection<FinalizationData> {
        return finalizationDetector.getFinalizations()
    }

    override fun addFinalization(finalization: FinalizationData, upstreamId: String) {
        if (getId() == upstreamId) {
            finalizationDetector.addFinalization(finalization)
        }
    }

    private fun detectFinalization() {
        finalizationDetectorSubscription =
            finalizationDetector.detectFinalization(this, chainConfig.expectedBlockTime, getChain()).subscribe {
                sendUpstreamStateEvent(UPDATED)
            }
    }

    private fun detectLowerBlock() {
        lowerBlockDetectorSubscription =
            lowerBoundService.detectLowerBounds()
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
}
