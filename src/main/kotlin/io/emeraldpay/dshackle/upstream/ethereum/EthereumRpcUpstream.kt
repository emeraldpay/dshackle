package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MergedPowHead
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.WithHttpStatus
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import java.time.Duration

open class EthereumRpcUpstream(
    id: String,
    val chain: Chain,
    forkWatch: ForkWatch,
    private val directReader: StandardRpcReader,
    private val wsPool: WsConnectionPool? = null,
    options: UpstreamsConfig.Options,
    role: UpstreamsConfig.UpstreamRole,
    private val node: QuorumForLabels.QuorumItem,
    targets: CallMethods
) : EthereumUpstream(id, chain, forkWatch, options, role, targets, node), Upstream, CachesEnabled, Lifecycle {

    constructor(id: String, chain: Chain, directReader: StandardRpcReader, options: UpstreamsConfig.Options, role: UpstreamsConfig.UpstreamRole, node: QuorumForLabels.QuorumItem, targets: CallMethods) :
        this(
            id, chain, ForkWatch.Never(), directReader, null, options, role, node, targets
        )

    constructor(id: String, chain: Chain, forkWatch: ForkWatch, api: StandardRpcReader) :
        this(
            id, chain, forkWatch, api, null,
            UpstreamsConfig.PartialOptions.getDefaults().build(), UpstreamsConfig.UpstreamRole.PRIMARY,
            QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels()),
            DirectCallMethods()
        )

    constructor(id: String, chain: Chain, api: StandardRpcReader) :
        this(id, chain, ForkWatch.Never(), api)

    private val log = LoggerFactory.getLogger(EthereumRpcUpstream::class.java)

    private val head: Head = this.createHead()
    private var validatorSubscription: Disposable? = null

    init {
        if (directReader is WithHttpStatus) {
            directReader.onHttpError = this.watchHttpCodes
        }
    }

    override fun setCaches(caches: Caches) {
        if (head is CachesEnabled) {
            head.setCaches(caches)
        }
    }

    override fun getIngressSubscription(): EthereumIngressSubscription {
        return NoEthereumIngressSubscription.DEFAULT
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")
        super.start()
        if (getOptions().disableValidation) {
            log.warn("Disable validation for upstream ${this.getId()}")
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            log.debug("Start validation for upstream ${this.getId()}")
            val validator = EthereumUpstreamValidator(this, getOptions())
            validatorSubscription = validator.start()
                .subscribe(this::setStatus)
        }
    }

    override fun isRunning(): Boolean {
        return true
    }

    override fun stop() {
        super.stop()
        validatorSubscription?.dispose()
        validatorSubscription = null
        if (head is Lifecycle) {
            head.stop()
        }
    }

    open fun createHead(): Head {
        return if (wsPool != null) {
            val subscriptions = WsSubscriptionsImpl(wsPool)
            val wsHead = EthereumWsHead(chain, getIngressReader(), subscriptions).apply {
                start()
            }
            // receive all new blocks through WebSockets, but also periodically verify with RPC in case if WS failed
            val rpcHead = EthereumRpcHead(chain, getIngressReader(), Duration.ofSeconds(60)).apply {
                start()
            }
            MergedPowHead(listOf(rpcHead, wsHead)).apply {
                start()
            }
        } else {
            log.warn("Setting up upstream ${this.getId()} with RPC-only access, less effective than WS+RPC")
            EthereumRpcHead(chain, getIngressReader()).apply {
                start()
            }
        }
    }

    override fun getHead(): Head {
        return head
    }

    override fun getIngressReader(): StandardRpcReader {
        return directReader
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
