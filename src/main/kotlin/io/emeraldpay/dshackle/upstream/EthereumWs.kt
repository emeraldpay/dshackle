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

class EthereumWs(
        private val uri: URI,
        private val origin: URI
) {

    private val log = LoggerFactory.getLogger(EthereumWs::class.java)
    private val topic = TopicProcessor
            .builder<BlockJson<TransactionId>>()
            .name("new-blocks")
            .build()
    private val head = AtomicReference<BlockJson<TransactionId>>(null)

    fun connect() {
        log.info("Connecting to WebSocket: $uri")
        val client = WebsocketClient()
        try {
            client.connect(uri, origin)
        } catch (e: Exception) {
            log.error("Failed to connect to websocket at $uri. Error: ${e.message}")
            return
        }
        client.onNewBlock {
            topic.onNext(it)
        }

        getFlux().subscribe { head.set(it) }
    }

    fun getFlux(): Flux<BlockJson<TransactionId>> {
        return Flux.from(this.topic)
                .onBackpressureLatest()
                .sample(Duration.ofMillis(100))
    }

    fun getHead(): BlockJson<TransactionId>? {
        return head.get()
    }
}