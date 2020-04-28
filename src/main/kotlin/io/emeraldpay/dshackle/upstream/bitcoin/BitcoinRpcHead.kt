package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.Head
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors

class BitcoinRpcHead(
        private val api: DirectBitcoinApi,
        private val extractBlock: ExtractBlock,
        private val interval: Duration = Duration.ofSeconds(15)
) : Head, AbstractHead(), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinRpcHead::class.java)
        val scheduler = Schedulers.fromExecutor(Executors.newCachedThreadPool(CustomizableThreadFactory("bitcoin-rpc-head")))
    }

    private var refreshSubscription: Disposable? = null

    override fun isRunning(): Boolean {
        return refreshSubscription != null
    }

    override fun start() {
        if (refreshSubscription != null) {
            log.warn("Called to start when running")
            return
        }
        val base = Flux.interval(interval)
                .publishOn(scheduler)
                .flatMap {
                    api.executeAndResult(0, "getbestblockhash", emptyList(), String::class.java)
                            .timeout(Defaults.timeout, Mono.error(Exception("Best block hash is not received")))
                }
                .distinctUntilChanged()
                .flatMap { hash ->
                    api.execute(0, "getblock", listOf(hash))
                            .map(extractBlock::extract)
                            .timeout(Defaults.timeout, Mono.error(Exception("Block data is not received")))
                }
                .onErrorContinue { err, _ ->
                    log.debug("RPC error ${err.message}")
                }
        refreshSubscription = super.follow(base)
    }

    override fun stop() {
        val copy = refreshSubscription
        refreshSubscription = null
        copy?.dispose()
    }

}