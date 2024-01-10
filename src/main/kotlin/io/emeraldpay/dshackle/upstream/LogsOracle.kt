package io.emeraldpay.dshackle.upstream

import io.emeraldpay.dshackle.config.IndexConfig
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

class LogsOracle(
    private val config: IndexConfig.Index,
    private val upstream: Multistream,
    private val scheduler: Scheduler,
) {

    private val log = LoggerFactory.getLogger(LogsOracle::class.java)

    private var subscription: Disposable? = null
    private val db = org.drpc.logsoracle.LogsOracle(config.store, config.ram_limit ?: 0L)

    fun start() {
        db.SetUpstream(config.rpc)

        subscription = upstream.getHead().getFlux()
            .doOnError { t -> log.warn("Failed to subscribe head for oracle", t) }
            .subscribe { db.UpdateHeight(it.height) }
    }

    fun stop() {
        db.close()

        subscription?.dispose()
        subscription = null
    }

    fun estimate(
        limit: Long?,
        fromBlock: Long,
        toBlock: Long,
        address: List<String>,
        topics: List<List<String>>,
    ): Mono<String> {
        return Mono.fromCallable {
            try {
                val estimate = db.Query(limit, fromBlock, toBlock, address, topics)
                "{\"total\":$estimate,\"overflow\":false}"
            } catch (e: org.drpc.logsoracle.LogsOracle.LogsOracleException) {
                if (e.isQueryOverflow()) {
                    "{\"total\":\"-1\",\"overflow\":true}"
                } else {
                    throw e
                }
            }
        }
            .subscribeOn(scheduler)
    }
}
