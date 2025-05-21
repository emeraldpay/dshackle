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
import io.emeraldpay.dshackle.upstream.UpstreamRpcMethodsDetector
import io.emeraldpay.dshackle.upstream.UpstreamRpcMethodsDetectorBuilder
import io.emeraldpay.dshackle.upstream.UpstreamSettingsDetectorBuilder
import io.emeraldpay.dshackle.upstream.UpstreamValidator
import io.emeraldpay.dshackle.upstream.UpstreamValidatorBuilder
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_FATAL_SETTINGS_ERROR
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_SETTINGS_ERROR
import io.emeraldpay.dshackle.upstream.ValidateUpstreamSettingsResult.UPSTREAM_VALID
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.ethereum.HeadLivenessState
import io.emeraldpay.dshackle.upstream.finalization.FinalizationData
import io.emeraldpay.dshackle.upstream.generic.connectors.ConnectorFactory
import io.emeraldpay.dshackle.upstream.generic.connectors.GenericConnector
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundData
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundServiceBuilder
import io.emeraldpay.dshackle.upstream.lowerbound.LowerBoundType
import org.springframework.context.Lifecycle
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

open class GenericUpstream(
    id: String,
    chain: Chain,
    hash: Short,
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
) : DefaultUpstream(id, hash, null, UpstreamAvailability.OK, options, role, targets, node, chainConfig, chain),
    Lifecycle {
    constructor(
        config: UpstreamsConfig.Upstream<*>,
        chain: Chain,
        hash: Short,
        options: ChainOptions.Options,
        node: QuorumForLabels.QuorumItem?,
        chainConfig: ChainsConfig.ChainConfig,
        connectorFactory: ConnectorFactory,
        validatorBuilder: UpstreamValidatorBuilder,
        upstreamSettingsDetectorBuilder: UpstreamSettingsDetectorBuilder,
        upstreamRpcMethodsDetectorBuilder: UpstreamRpcMethodsDetectorBuilder,
        buildMethods: (UpstreamsConfig.Upstream<*>, Chain) -> CallMethods,
        lowerBoundServiceBuilder: LowerBoundServiceBuilder,
        finalizationDetectorBuilder: FinalizationDetectorBuilder,
        versionRules: Supplier<CompatibleVersionsRules?>,
    ) : this(
        config.id!!,
        chain,
        hash,
        options,
        config.role,
        buildMethods(config, chain),
        node,
        chainConfig,
        connectorFactory,
        validatorBuilder,
        upstreamSettingsDetectorBuilder,
        lowerBoundServiceBuilder,
        finalizationDetectorBuilder,
        versionRules,
    ) {
        rpcMethodsDetector = upstreamRpcMethodsDetectorBuilder(this, config)
        detectRpcMethods(config, buildMethods)
    }

    private val validator: UpstreamValidator? = validatorBuilder(chain, this, getOptions(), chainConfig, versionRules)
    private val validatorSubscription = AtomicReference<Disposable?>()
    private val validationSettingsSubscription = AtomicReference<Disposable?>()
    private val lowerBlockDetectorSubscription = AtomicReference<Disposable?>()
    private val settingsDetectorSubscription = AtomicReference<Disposable?>()

    private val hasLiveSubscriptionHead: AtomicBoolean = AtomicBoolean(getOptions().disableLivenessSubscriptionValidation)
    protected val connector: GenericConnector = connectorFactory.create(this, chain)
    private val livenessSubscription = AtomicReference<Disposable?>()
    private val settingsDetector = upstreamSettingsDetectorBuilder(chain, this)
    private var rpcMethodsDetector: UpstreamRpcMethodsDetector? = null

    private val lowerBoundService = lowerBoundServiceBuilder(chain, this)

    private val started = AtomicBoolean(false)
    private val isUpstreamValid = AtomicBoolean(false)
    private val clientVersion = AtomicReference(UNKNOWN_CLIENT_VERSION)

    private val finalizationDetector = finalizationDetectorBuilder()
    private val finalizationDetectorSubscription = AtomicReference<Disposable?>()

    private val headLivenessState = Sinks.many().multicast().directBestEffort<ValidateUpstreamSettingsResult>()

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
                UPSTREAM_FATAL_SETTINGS_ERROR -> {
                    log.warn("Upstream ${getId()} couldn't start, invalid upstream settings")
                    connector.stop()
                    return
                }

                UPSTREAM_SETTINGS_ERROR -> {
                    log.warn("Non fatal upstream settings error, continue validation...")
                    connector.getHead().stop()
                }

                UPSTREAM_VALID -> {
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
            validationSettingsSubscription.set(
                Flux.merge(
                    Flux.interval(
                        Duration.ofSeconds(20),
                    ).flatMap {
                        validator.validateUpstreamSettings()
                    },
                    headLivenessState.asFlux(),
                )
                    .subscribeOn(upstreamSettingsScheduler)
                    .distinctUntilChanged()
                    .subscribe {
                        when (it) {
                            UPSTREAM_FATAL_SETTINGS_ERROR -> {
                                if (isUpstreamValid.get()) {
                                    log.warn("There is a fatal error after upstream settings validation, removing ${getId()}...")
                                    partialStop()
                                    sendUpstreamStateEvent(UpstreamChangeEvent.ChangeType.FATAL_SETTINGS_ERROR_REMOVED)
                                }
                                isUpstreamValid.set(false)
                            }

                            UPSTREAM_VALID -> {
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
                    },
            )
        }
    }

    private fun detectSettings() {
        if (settingsDetector != null) {
            settingsDetectorSubscription.set(
                Flux.interval(
                    Duration.ZERO,
                    Duration.ofSeconds(getOptions().validationInterval.toLong() * 5),
                ).flatMap {
                    Flux.merge(
                        settingsDetector.detectLabels()
                            .doOnNext { label ->
                                updateLabels(label)
                                sendUpstreamStateEvent(UPDATED)
                            },
                        settingsDetector.detectClientVersion()
                            .doOnNext {
                                log.info("Detected node version $it for upstream ${getId()}")
                                clientVersion.set(it)
                            },
                    )
                        .subscribeOn(settingsScheduler)
                }.subscribe(),
            )
        }
    }

    private fun detectRpcMethods(
        config: UpstreamsConfig.Upstream<*>,
        buildMethods: (UpstreamsConfig.Upstream<*>, Chain) -> CallMethods,
    ) {
        try {
            rpcMethodsDetector?.detectRpcMethods()?.subscribe { rpcDetector ->
                log.info("Upstream rpc method detector for  ${getId()} returned  $rpcDetector ")
                if (rpcDetector.isEmpty()) {
                    return@subscribe
                }
                if (config.methods == null) {
                    config.methods = UpstreamsConfig.Methods(mutableSetOf(), mutableSetOf())
                }
                val enableMethods =
                    rpcDetector
                        .filter { (_, enabled) -> enabled }
                        .keys
                        .map { UpstreamsConfig.Method(it) }
                        .toSet()
                val disableMethods =
                    rpcDetector
                        .filter { (_, enabled) -> !enabled }
                        .keys
                        .map { UpstreamsConfig.Method(it) }
                        .toSet()
                config.methods =
                    UpstreamsConfig.Methods(
                        enableMethods
                            .minus(disableMethods)
                            .plus(config.methods!!.enabled),
                        disableMethods
                            .minus(enableMethods)
                            .plus(config.methods!!.disabled),
                    )
                updateMethods(buildMethods(config, getChain()))
            }
        } catch (e: Exception) {
            log.error("Couldn't detect methods of upstream ${getId()} due to error {}", e.message)
        }
    }

    private fun upstreamStart() {
        if (getOptions().disableValidation) {
            log.warn("Disable validation for upstream ${this.getId()}")
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            log.debug("Start validation for upstream ${this.getId()}")
            validatorSubscription.set(
                validator?.start()
                    ?.subscribe(this::setStatus),
            )
        }
        if (!getOptions().disableLivenessSubscriptionValidation) {
            livenessSubscription.set(
                connector.headLivenessEvents().subscribe(
                    {
                        val hasSub = it == HeadLivenessState.OK
                        hasLiveSubscriptionHead.set(hasSub)
                        if (it == HeadLivenessState.FATAL_ERROR) {
                            headLivenessState.emitNext(UPSTREAM_FATAL_SETTINGS_ERROR) { _, res -> res == Sinks.EmitResult.FAIL_NON_SERIALIZED }
                        } else {
                            sendUpstreamStateEvent(UPDATED)
                        }
                    },
                    {
                        log.debug("Error while checking live subscription for ${getId()}", it)
                    },
                ),
            )
        }
        detectSettings()

        detectLowerBlock()

        detectFinalization()
    }

    override fun stop() {
        partialStop()
        validationSettingsSubscription.getAndSet(null)?.dispose()
        connector.stop()
        started.set(false)
    }

    private fun partialStop() {
        validatorSubscription.getAndSet(null)?.dispose()
        livenessSubscription.getAndSet(null)?.dispose()
        lowerBlockDetectorSubscription.getAndSet(null)?.dispose()
        finalizationDetectorSubscription.getAndSet(null)?.dispose()
        settingsDetectorSubscription.getAndSet(null)?.dispose()
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
        finalizationDetectorSubscription.set(
            finalizationDetector.detectFinalization(this, chainConfig.expectedBlockTime, getChain())
                .subscribeOn(finalizationScheduler)
                .subscribe {
                    sendUpstreamStateEvent(UPDATED)
                },
        )
    }

    private fun detectLowerBlock() {
        lowerBlockDetectorSubscription.set(
            lowerBoundService.detectLowerBounds()
                .subscribeOn(lowerScheduler)
                .subscribe {
                    sendUpstreamStateEvent(UPDATED)
                },
        )
    }

    fun getIngressSubscription(): IngressSubscription {
        return connector.getIngressSubscription()
    }

    override fun isRunning() = connector.isRunning() || started.get()

    override fun updateLowerBound(lowerBound: Long, type: LowerBoundType) {
        lowerBoundService.updateLowerBound(lowerBound, type)
    }

    override fun predictLowerBound(type: LowerBoundType): Long {
        return lowerBoundService.predictLowerBound(type)
    }

    fun isValid(): Boolean = isUpstreamValid.get()

    companion object {
        private val upstreamSettingsScheduler =
            Schedulers.fromExecutor(Executors.newFixedThreadPool(4, CustomizableThreadFactory("upstreamSettings-")))
        private val finalizationScheduler =
            Schedulers.fromExecutor(Executors.newFixedThreadPool(4, CustomizableThreadFactory("finalization-")))
        private val settingsScheduler =
            Schedulers.fromExecutor(Executors.newFixedThreadPool(4, CustomizableThreadFactory("settings-")))
        private val lowerScheduler =
            Schedulers.fromExecutor(Executors.newFixedThreadPool(4, CustomizableThreadFactory("lowerBound-")))
    }
}
