package io.emeraldpay.dshackle.upstream.polkadot

import io.emeraldpay.dshackle.upstream.IngressSubscription
import io.emeraldpay.dshackle.upstream.SubscriptionConnect
import io.emeraldpay.dshackle.upstream.ethereum.WsSubscriptions
import io.emeraldpay.dshackle.upstream.generic.subscribe.GenericPersistentConnect
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

class PolkadotIngressSubscription(val conn: WsSubscriptions) : IngressSubscription {
    override fun getAvailableTopics(): List<String> {
        return emptyList() // not used now
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> get(topic: String): SubscriptionConnect<T> {
        return PolkaConnect(conn, topic) as SubscriptionConnect<T>
    }
}

class PolkaConnect(
    val conn: WsSubscriptions,
    val topic: String,
) : GenericPersistentConnect() {

    @Suppress("UNCHECKED_CAST")
    override fun createConnection(): Flux<Any> {
        return conn.subscribe(JsonRpcRequest(topic, listOf()))
            .data
            .timeout(Duration.ofSeconds(60), Mono.empty())
            .onErrorResume { Mono.empty() } as Flux<Any>
    }
}
