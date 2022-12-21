package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectLogs
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectNewHeads
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectSyncing
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.hex.Hex32
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

open class EthereumSubscriptionApi(
    val upstream: EthereumLikeMultistream,
    val pendingTxesSource: PendingTxesSource
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumSubscriptionApi::class.java)

        const val METHOD_NEW_HEADS = "newHeads"
        const val METHOD_LOGS = "logs"
        const val METHOD_SYNCING = "syncing"
        const val METHOD_PENDING_TXES = "newPendingTransactions"
    }

    private val newHeads = ConnectNewHeads(upstream)
    open val logs = ConnectLogs(upstream)
    private val syncing = ConnectSyncing(upstream)

    @Suppress("UNCHECKED_CAST")
    open fun subscribe(method: String, params: Any?, matcher: Selector.Matcher): Flux<out Any> {
        if (method == METHOD_NEW_HEADS) {
            return newHeads.connect(matcher)
        }
        if (method == METHOD_LOGS) {
            val paramsMap = try {
                if (params != null && Map::class.java.isAssignableFrom(params.javaClass)) {
                    readLogsRequest(params as Map<String, Any?>)
                } else {
                    LogsRequest(emptyList(), emptyList())
                }
            } catch (t: Throwable) {
                return Flux.error(UnsupportedOperationException("Invalid parameter for $method. Error: ${t.message}"))
            }
            return logs.create(paramsMap.address, paramsMap.topics).connect(matcher)
        }
        if (method == METHOD_SYNCING) {
            return syncing.connect(matcher)
        }
        if (method == METHOD_PENDING_TXES) {
            return pendingTxesSource.connect(matcher)
        }
        return Flux.error(UnsupportedOperationException("Method $method is not supported"))
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
                else -> throw IllegalArgumentException("Invalid type of topics field. Must be string or list of strings")
            }
        } else {
            emptyList()
        }
        return LogsRequest(addresses, topics)
    }
}
