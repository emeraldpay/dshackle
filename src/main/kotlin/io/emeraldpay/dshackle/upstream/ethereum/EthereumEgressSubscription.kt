package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectLogs
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectNewHeads
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectSyncing
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.hex.Hex32
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler

open class EthereumEgressSubscription(
    val upstream: EthereumLikeMultistream,
    val scheduler: Scheduler,
    val pendingTxesSource: PendingTxesSource?
) : EgressSubscription {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumEgressSubscription::class.java)

        const val METHOD_NEW_HEADS = "newHeads"
        const val METHOD_LOGS = "logs"
        const val METHOD_SYNCING = "syncing"
        const val METHOD_PENDING_TXES = "newPendingTransactions"
    }

    private val availableTopics = listOf(
        METHOD_NEW_HEADS,
        METHOD_LOGS,
        METHOD_SYNCING,
    ).let {
        if (pendingTxesSource != null) {
            it + METHOD_PENDING_TXES
        } else {
            it
        }
    }

    private val newHeads = ConnectNewHeads(upstream, scheduler)
    open val logs = ConnectLogs(upstream, scheduler)
    private val syncing = ConnectSyncing(upstream)

    override fun getAvailableTopics() = availableTopics

    @Suppress("UNCHECKED_CAST")
    override fun subscribe(topic: String, params: Any?, matcher: Selector.Matcher): Flux<out Any> {
        if (topic == METHOD_NEW_HEADS) {
            return newHeads.connect(matcher)
        }
        if (topic == METHOD_LOGS) {
            val paramsMap = try {
                if (params != null && Map::class.java.isAssignableFrom(params.javaClass)) {
                    readLogsRequest(params as Map<String, Any?>)
                } else {
                    LogsRequest(emptyList(), emptyList())
                }
            } catch (t: Throwable) {
                return Flux.error(UnsupportedOperationException("Invalid parameter for $topic. Error: ${t.message}"))
            }
            return logs.create(paramsMap.address, paramsMap.topics).connect(matcher)
        }
        if (topic == METHOD_SYNCING) {
            return syncing.connect(matcher)
        }
        if (topic == METHOD_PENDING_TXES) {
            return pendingTxesSource?.connect(matcher) ?: Flux.empty()
        }
        return Flux.error(UnsupportedOperationException("Method $topic is not supported"))
    }

    data class LogsRequest(
        val address: List<Address>,
        val topics: List<Hex32>
    )

    fun readLogsRequest(params: Map<String, Any?>): LogsRequest {
        val addresses: List<Address> = if (params.containsKey("address")) {
            when (val address = params["address"]) {
                is String -> try {
                    listOf(Address.from(address))
                } catch (t: Throwable) {
                    log.debug("Ignore invalid address: $address with error ${t.message}")
                    emptyList()
                }
                is Collection<*> -> address.mapNotNull {
                    try {
                        Address.from(it.toString())
                    } catch (t: Throwable) {
                        log.debug("Ignore invalid address: $address with error ${t.message}")
                        null
                    }
                }
                null -> emptyList()
                else -> throw IllegalArgumentException("Invalid type of address field. Must be string or list of strings")
            }
        } else {
            emptyList()
        }
        val topics: List<Hex32> = if (params.containsKey("topics")) {
            when (val topics = params["topics"]) {
                is String -> try {
                    listOf(Hex32.from(topics))
                } catch (t: Throwable) {
                    log.debug("Ignore invalid topic: $topics with error ${t.message}")
                    emptyList()
                }
                is Collection<*> -> topics.mapNotNull {
                    try {
                        Hex32.from(it.toString())
                    } catch (t: Throwable) {
                        log.debug("Ignore invalid topic: $topics with error ${t.message}")
                        null
                    }
                }
                null -> emptyList()
                else -> throw IllegalArgumentException("Invalid type of topics field. Must be string or list of strings")
            }
        } else {
            emptyList()
        }
        return LogsRequest(addresses, topics)
    }
}
