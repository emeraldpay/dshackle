package io.emeraldpay.dshackle.upstream.generic

import io.emeraldpay.dshackle.upstream.ChainRequest
import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.generic.subscribe.GenericPersistentConnect
import io.emeraldpay.dshackle.upstream.rpcclient.ListParams
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.log

class GenericIngressSubscription(val conn: WsSubscriptions, val methods: List<String>) : IngressSubscription {
    override fun getAvailableTopics(): List<String> {
        return methods
    }

    private val holders = ConcurrentHashMap<Pair<String, Any?>, SubscriptionConnect<out Any>>()

    @Suppress("UNCHECKED_CAST")
    override fun <T> get(topic: String, params: Any?): SubscriptionConnect<T> {
        return holders.computeIfAbsent(topic to params) { key ->
            GenericSubscriptionConnect(
                conn,
                key.first,
                key.second,
            )
        } as SubscriptionConnect<T>
    }
}

class GenericSubscriptionConnect(
    val conn: WsSubscriptions,
    val topic: String,
    val params: Any?,
) : GenericPersistentConnect() {

    companion object {
        private val log = LoggerFactory.getLogger(GenericSubscriptionConnect::class.java)
    }

    @Suppress("UNCHECKED_CAST")
    override fun createConnection(): Flux<Any> {
        return conn.subscribe(ChainRequest(topic, ListParams(getParams(params) as List<Any>)))
            .data
            .timeout(
                Duration.ofSeconds(60),
                Mono.empty<ByteArray?>().doOnEach {
                    log.warn("Timeout during subscription to $topic")
                },
            )
            .onErrorResume {
                log.error("Error during subscription to $topic", it)
                Mono.empty()
            } as Flux<Any>
    }

    private fun getParams(params: Any?): List<Any?> {
        if (params == null) {
            return listOf()
        }
        return params as List<Any?>
    }
}
