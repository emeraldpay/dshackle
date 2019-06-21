package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class Upstream(
        val chain: Chain,
        val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null,
        private val options: UpstreamsConfig.Options
) {

    private val log = LoggerFactory.getLogger(Upstream::class.java)

    val head: EthereumHead = if (ethereumWs != null) {
        EthereumWsHead(ethereumWs)
    } else {
        EthereumRpcHead(api).apply {
            this.start()
        }
    }

    val validator = UpstreamValidator(this, options)
    private val status = AtomicReference(UpstreamAvailability.UNAVAILABLE)

    init {
        log.info("Configured for ${chain.chainName}")

        validator.start()
                .subscribe {
                    status.set(it)
                }
    }

    fun isAvailable(): Boolean {
        return status.get() == UpstreamAvailability.OK
    }

    fun getStatus(): UpstreamAvailability {
        return status.get()
    }
}