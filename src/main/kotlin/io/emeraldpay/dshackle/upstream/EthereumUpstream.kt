package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.grpc.Chain
import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.util.concurrent.atomic.AtomicReference

class EthereumUpstream(
        val chain: Chain,
        private val api: EthereumApi,
        private val ethereumWs: EthereumWs? = null,
        private val options: UpstreamsConfig.Options
): Upstream {

    override fun getSupportedTargets(): Set<String> {
        return api.getSupportedMethods()
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
    private val status = AtomicReference(UpstreamAvailability.UNAVAILABLE)
    private val statusStream: TopicProcessor<UpstreamAvailability> = TopicProcessor.create()

    init {
        log.info("Configured for ${chain.chainName}")

        validator.start()
                .subscribe {
                    status.set(it)
                    statusStream.onNext(it)
                }
    }

    override fun isAvailable(): Boolean {
        return status.get() == UpstreamAvailability.OK
    }

    override fun getStatus(): UpstreamAvailability {
        return status.get()
    }

    override fun observeStatus(): Flux<UpstreamAvailability> {
        return Flux.from(statusStream)
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