package io.emeraldpay.dshackle.upstream.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.config.UpstreamsConfig
import io.emeraldpay.dshackle.startup.QuorumForLabels
import io.emeraldpay.dshackle.upstream.*
import io.emeraldpay.dshackle.upstream.calls.CallMethods
import io.emeraldpay.grpc.Chain
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.Disposable
import reactor.core.publisher.Mono

open class BitcoinUpstream(
        id: String,
        val chain: Chain,
        private val api: DirectBitcoinApi,
        options: UpstreamsConfig.Options,
        val node: QuorumForLabels.QuorumItem,
        private val objectMapper: ObjectMapper,
        callMethods: CallMethods
) : DefaultUpstream<DirectBitcoinApi>(id, options, callMethods), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinUpstream::class.java)
    }

    private val head: Head = createHead()
    private var validatorSubscription: Disposable? = null
    private val data = BitcoinData(api, head)

    private fun createHead(): Head {
        return BitcoinRpcHead(
                api,
                ExtractBlock(objectMapper)
        )
    }

    open fun getData(): BitcoinData {
        return data
    }

    override fun getHead(): Head {
        return head
    }

    override fun getApi(matcher: Selector.Matcher): Mono<out DirectBitcoinApi> {
        return Mono.just(api)
    }

    override fun getLabels(): Collection<UpstreamsConfig.Labels> {
        return listOf(UpstreamsConfig.Labels())
    }

    override fun <T : Upstream<TA>, TA : UpstreamApi> cast(selfType: Class<T>, apiType: Class<TA>): T {
        if (!selfType.isAssignableFrom(this.javaClass)) {
            throw ClassCastException("Cannot cast ${this.javaClass} to $selfType")
        }
        return castApi(apiType) as T
    }

    override fun <A : UpstreamApi> castApi(apiType: Class<A>): Upstream<A> {
        if (!apiType.isAssignableFrom(DirectBitcoinApi::class.java)) {
            throw ClassCastException("Cannot cast ${DirectBitcoinApi::class.java} to $apiType")
        }
        return this as Upstream<A>
    }

    override fun isRunning(): Boolean {
        var runningAny = validatorSubscription != null
        if (head is Lifecycle) {
            runningAny = runningAny || head.isRunning
        }
        runningAny = runningAny || data.isRunning
        return runningAny
    }

    override fun start() {
        log.info("Configured for ${chain.chainName}")
        if (head is Lifecycle) {
            if (!head.isRunning) {
                head.start()
            }
        }
        data.start()

        validatorSubscription?.dispose()

        if (getOptions().disableValidation != null && getOptions().disableValidation!!) {
            this.setLag(0)
            this.setStatus(UpstreamAvailability.OK)
        } else {
            val validator = BitcoinUpstreamValidator(api, getOptions())
            validatorSubscription = validator.start()
                    .subscribe(this::setStatus)
        }
    }

    override fun stop() {
        if (head is Lifecycle) {
            head.stop()
        }
        data.stop()
        validatorSubscription?.dispose()
    }


}