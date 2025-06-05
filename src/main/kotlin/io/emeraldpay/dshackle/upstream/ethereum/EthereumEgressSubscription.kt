package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.Global
import io.emeraldpay.dshackle.upstream.Capability
import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.EgressSubscription
import io.emeraldpay.dshackle.upstream.Multistream
import io.emeraldpay.dshackle.upstream.Selector
import io.emeraldpay.dshackle.upstream.ethereum.domain.Address
import io.emeraldpay.dshackle.upstream.ethereum.hex.Hex32
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectLogs
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.ConnectNewHeads
import io.emeraldpay.dshackle.upstream.ethereum.subscribe.PendingTxesSource
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

data class Transaction(
    val blockHash: String?,
    val blockNumber: String?,
    val from: String,
    val gas: String,

    val gasPrice: String?,
    val maxFeePerGas: String?,

    val maxPriorityFeePerGas: String?,

    val hash: String,
    val input: String,
    val nonce: String,
    val to: String,
    val transactionIndex: String?,
    val value: String,
    val type: String,
    val accessList: List<Any>?,
    val chainId: String,
    val v: String,
    val yParity: String?,
    val r: String?,
    val s: String?,
)

open class EthereumEgressSubscription(
    val upstream: Multistream,
    val scheduler: Scheduler,
    val pendingTxesSource: PendingTxesSource?,
) : EgressSubscription {

    companion object {
        private val log = LoggerFactory.getLogger(EthereumEgressSubscription::class.java)

        const val METHOD_NEW_HEADS = "newHeads"
        const val METHOD_LOGS = "logs"
        const val METHOD_PENDING_TXES = "newPendingTransactions"
        const val METHOD_PENDING_TXES_WITH_BODY = "newPendingTransactionsWithBody"
    }

    private val newHeads = ConnectNewHeads(upstream, scheduler)
    open val logs = ConnectLogs(upstream, scheduler)
    override fun getAvailableTopics(): List<String> {
        val subs = if (upstream.getCapabilities().contains(Capability.WS_HEAD)) {
            // we can't use logs without eth_getLogs method available
            if (upstream.getMethods().isAvailable("eth_getLogs")) {
                listOf(METHOD_NEW_HEADS, METHOD_LOGS)
            } else {
                listOf(METHOD_NEW_HEADS)
            }
        } else {
            listOf()
        }
        return if (pendingTxesSource != null) {
            subs.plus(listOf(METHOD_PENDING_TXES, METHOD_PENDING_TXES_WITH_BODY))
        } else {
            subs
        }
    }

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
        if (topic == METHOD_PENDING_TXES) {
            return pendingTxesSource?.connect(matcher) ?: Flux.empty()
        } else if (topic == METHOD_PENDING_TXES_WITH_BODY) {
            return pendingTxesSource?.connect(matcher)?.flatMap { txHash ->
                // Create request to get full transaction
                val request = ChainRequest(
                    "eth_getTransactionByHash",
                    ListParams(txHash.toString()),
                )

                // try to read froms each upstream
                Flux.fromIterable(upstream.getUpstreams())
                    .flatMap { currentUpstream ->
                        currentUpstream.getIngressReader().read(request)
                            .timeout(Defaults.internalCallsTimeout)
                            .flatMap { response ->
                                val result = response.getResult()
                                if (result.isEmpty()) {
                                    Mono.empty()
                                } else {
                                    Mono.just(Global.objectMapper.readValue(result, Transaction::class.java))
                                }
                            }
                            .doOnError { err ->
                                log.debug("Failed to get response from upstream ${currentUpstream.getId()} tx: $txHash: ${err.message}")
                            }
                            .onErrorResume { Mono.empty() }
                    }
                    .next()
                    .doOnSuccess { resp -> log.debug("eth_getTransactionByHash got response: $resp") }
                    .doOnError { err -> log.warn("eth_getTransactionByHash failed to get response from any upstream tx: $txHash: ${err.message}") }
            } ?: Flux.empty()
        }
        return Flux.error(UnsupportedOperationException("Method $topic is not supported"))
    }

    data class LogsRequest(
        val address: List<Address>,
        val topics: List<Hex32?>,
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
        val topics: List<Hex32?> = if (params.containsKey("topics")) {
            when (val topics = params["topics"]) {
                is String -> try {
                    listOf(Hex32.from(topics))
                } catch (t: Throwable) {
                    log.debug("Ignore invalid topic: $topics with error ${t.message}")
                    emptyList()
                }
                is Collection<*> -> topics.map { topic ->
                    try {
                        when (topic) {
                            null -> null
                            is Collection<*> -> topic.firstOrNull()?.toString()?.let { Hex32.from(it) }
                            else -> topic?.toString()?.let { Hex32.from(it) }
                        }
                    } catch (t: Throwable) {
                        log.debug("Ignore invalid topic: $topic with error ${t.message}")
                        throw IllegalArgumentException("Invalid topic: $topic")
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
