package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class EthereumUpstream(
        val chain: Chain,
        private val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null,
        private val options: UpstreamsConfig.Options
): Upstream {

    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    private val head: EthereumHead = if (ethereumWs != null) {
        EthereumWsHead(ethereumWs)
    } else {
        EthereumRpcHead(api).apply {
            this.start()
        }
    }

    private val validator = UpstreamValidator(this, options)
    private val status = AtomicReference(UpstreamAvailability.UNAVAILABLE)

    init {
        log.info("Configured for ${chain.chainName}")

        validator.start()
                .subscribe {
                    status.set(it)
                }
    }

    override fun isAvailable(): Boolean {
        return status.get() == UpstreamAvailability.OK
    }

    override fun getStatus(): UpstreamAvailability {
        return status.get()
    }

    override fun getHead(): EthereumHead {
        return head
    }

    override fun getApi(): EthereumApi {
        return api
    }

    override fun getOptions(): UpstreamsConfig.Options {
        return options
    }
}