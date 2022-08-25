package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.ForkWatch
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.MergedHead
import io.emeraldpay.dshackle.upstream.Upstream
import io.emeraldpay.dshackle.upstream.UpstreamAvailability
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.dshackle.upstream.calls.DirectCallMethods
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import java.time.Duration

open class EthereumRpcUpstream(
    id: String,
    val chain: Chain,
    forkWatch: ForkWatch,
    private val directReader: Reader<JsonRpcRequest, JsonRpcResponse>,
    private val ethereumWsFactory: EthereumWsFactory? = null,
    options: UpstreamsConfig.Options,
    role: UpstreamsConfig.UpstreamRole,
    private val node: QuorumForLabels.QuorumItem,
    targets: CallMethods
) : EthereumUpstream(id, forkWatch, options, role, targets, node), Upstream, CachesEnabled, Lifecycle {

    constructor(id: String, chain: Chain, directReader: Reader<JsonRpcRequest, JsonRpcResponse>, options: UpstreamsConfig.Options, role: UpstreamsConfig.UpstreamRole, node: QuorumForLabels.QuorumItem, targets: CallMethods) :
        this(
            id, chain, ForkWatch.Never(), directReader, null, options, role, node, targets
        )

    constructor(id: String, chain: Chain, forkWatch: ForkWatch, api: Reader<JsonRpcRequest, JsonRpcResponse>) :
        this(
            id, chain, forkWatch, api, null,
            UpstreamsConfig.Options.getDefaults(), UpstreamsConfig.UpstreamRole.PRIMARY,
            QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels()),
            DirectCallMethods()
        )

    constructor(id: String, chain: Chain, api: Reader<JsonRpcRequest, JsonRpcResponse>) :
        this(id, chain, ForkWatch.Never(), api)

    private val log = LoggerFactory.getLogger(EthereumRpcUpstream::class.java)

    private val head: Head = this.createHead()
    private var validatorSubscription: Disposable? = null

    override fun setCaches(caches: Caches) {
        if (head is CachesEnabled) {
            head.setCaches(caches)
        }
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")
        super.start()
        if (getOptions().disableValidation != null && getOptions().disableValidation!!) {
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
        return if (ethereumWsFactory != null) {
            // do not set upstream to the WS, since it doesn't control the RPC upstream
            val ws = ethereumWsFactory.create(null, null).apply {
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
