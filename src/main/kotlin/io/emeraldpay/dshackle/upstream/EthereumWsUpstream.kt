package io.emeraldpay.dshackle.upstream

import io.infinitape.etherjar.domain.TransactionId
import io.infinitape.etherjar.rpc.json.BlockJson
import io.infinitape.etherjar.rpc.ws.WebsocketClient
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.TopicProcessor
import java.net.URI
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class EthereumWsUpstream(
        private val uri: URI,
        private val origin: URI
) {

    private val log = LoggerFactory.getLogger(EthereumWsUpstream::class.java)
    private val topic = TopicProcessor
            .builder<BlockJson<TransactionId>>()
            .name("new-blocks")
            .build()
    private val head = AtomicReference<BlockJson<TransactionId>>(null);

    fun connect() {
        log.info("Connecting to WebSocket: $uri")
        val client = WebsocketClient()
        client.connect(uri, origin)
        client.onNewBlock {
            topic.onNext(it)
        }

        topic
                .onBackpressureLatest()
                .sample(Duration.ofMillis(100))
                .subscribe { head.set(it) }
    }

    fun getFlux(): Flux<BlockJson<TransactionId>> {
        return this.topic
    }

    fun getHead(): BlockJson<TransactionId>? {
        return head.get()
    }
}