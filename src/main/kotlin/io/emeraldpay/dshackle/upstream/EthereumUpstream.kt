package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import java.io.Closeable

open class EthereumUpstream(
        val chain: Chain,
        private val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null,
        private val options: UpstreamsConfig.Options,
        val node: NodeDetailsList.NodeDetails,
        private val targets: CallMethods
): DefaultUpstream(), Lifecycle {

    constructor(chain: Chain, api: EthereumApi): this(chain, api, null,
            UpstreamsConfig.Options.getDefaults(), NodeDetailsList.NodeDetails(1, UpstreamsConfig.Labels()),
            DirectCallMethods())

    override fun getSupportedTargets(): Set<String> {
        return targets.getSupportedMethods()
    }

    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    private val head: EthereumHead = this.createHead()
    private var validatorSubscription: Disposable? = null

    init {
        api.upstream = this
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
    }

    open fun createHead(): EthereumHead {
        return if (ethereumWs != null) {
            EthereumWsHead(ethereumWs)
        } else {
            EthereumRpcHead(api).apply {
                this.start()
            }
        }
    }

    override fun isAvailable(matcher: Selector.Matcher): Boolean {
        return getStatus() == UpstreamAvailability.OK && matcher.matches(node.labels)
    }

    override fun getHead(): EthereumHead {
        return head
    }

    override fun getApi(matcher: Selector.Matcher): EthereumApi {
        return api
    }

    fun getApi(): EthereumApi {
        return api
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }

}