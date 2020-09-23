package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.cache.CachesEnabled
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.reader.Reader
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.*
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
        private val directReader: Reader<JsonRpcRequest, JsonRpcResponse>,
        private val ethereumWsFactory: EthereumWsFactory? = null,
        options: UpstreamsConfig.Options,
        role: UpstreamsConfig.UpstreamRole,
        private val node: QuorumForLabels.QuorumItem,
        targets: CallMethods
) : EthereumUpstream(id, options, role, targets, node), Upstream, CachesEnabled, Lifecycle {

    constructor(id: String, chain: Chain, api: Reader<JsonRpcRequest, JsonRpcResponse>) :
            this(id, chain, api, null,
                    UpstreamsConfig.Options.getDefaults(), UpstreamsConfig.UpstreamRole.STANDARD,
                    QuorumForLabels.QuorumItem(1, UpstreamsConfig.Labels()),
                    DirectCallMethods())


    private val log = LoggerFactory.getLogger(EthereumRpcUpstream::class.java)

    private val head: Head = this.createHead()
    private var validatorSubscription: Disposable? = null
    private val capabilities = if (options.providesBalance != false) {
        setOf(Capability.RPC, Capability.BALANCE)
    } else {
        setOf(Capability.RPC)
    }

    override fun setCaches(caches: Caches) {
        if (head is CachesEnabled) {
            head.setCaches(caches)
        }
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")

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
        validatorSubscription?.dispose()
        validatorSubscription = null
        if (head is Lifecycle) {
            head.stop()
        }
    }

    open fun createHead(): Head {
        return if (ethereumWsFactory != null) {
            val ws = ethereumWsFactory.create(this).apply {
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

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return listOf(node.labels)
    }

    override fun getCapabilities(): Set<Capability> {
        return capabilities
    }

    override fun isGrpc(): Boolean {
        return false
    }

    @Suppress("unchecked")
    override fun <T : Upstream> cast(selfType: Class<T>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return this as T
    }
}