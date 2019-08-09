package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

open class EthereumUpstream(
        val chain: Chain,
        private val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null,
        private val options: UpstreamsConfig.Options,
        val node: NodeDetailsList.NodeDetails,
        private val targets: EthereumTargets
): DefaultUpstream() {

    override fun getSupportedTargets(): Set<String> {
        return targets.getSupportedMethods()
    }

    private val log = LoggerFactory.getLogger(EthereumUpstream::class.java)

    private val head: EthereumHead = if (ethereumWs != null) {
        EthereumWsHead(ethereumWs)
    } else {
        EthereumRpcHead(api).apply {
            this.start()
        }
    }

    private val validator = UpstreamValidator(this, options)

    init {
        log.info("Configured for ${chain.chainName}")
        api.upstream = this

        validator.start()
                .subscribe(this::setStatus)
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