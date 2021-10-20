package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectLogs
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectNewHeads
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectSyncing
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.hex.Hex32
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

open class EthereumSubscribe(
    val upstream: EthereumMultistream
) {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumSubscribe::class.java)
    }

    private val newHeads = ConnectNewHeads(upstream)
    private val logs = ConnectLogs(upstream)
    private val syncing = ConnectSyncing(upstream)

    @Suppress("UNCHECKED_CAST")
    open fun subscribe(method: String, params: Any?): Flux<out Any> {
        if (method == "newHeads") {
            return newHeads.connect()
        }
        if (method == "logs") {
            val paramsMap = try {
                if (params != null && Map::class.java.isAssignableFrom(params.javaClass)) {
                    readLogsRequest(params as Map<String, Any?>)
                } else {
                    LogsRequest(emptyList(), emptyList())
                }
            } catch (t: Throwable) {
                return Flux.error(UnsupportedOperationException("Invalid parameter for $method. Error: ${t.message}"))
            }
            return logs.start(paramsMap.address, paramsMap.topics)
        }
        if (method == "syncing") {
            return syncing.connect()
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
