package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.Defaults
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.reader.JsonRpcReader
import io.emeraldpay.dshackle.upstream.AbstractHead
import io.emeraldpay.dshackle.upstream.Head
import io.emeraldpay.dshackle.upstream.Lifecycle
import io.emeraldpay.dshackle.upstream.forkchoice.MostWorkForkChoice
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

class BitcoinZMQHead(
    private val server: ZMQServer,
    private val api: JsonRpcReader,
    private val extractBlock: ExtractBlock,
) : Head, AbstractHead(MostWorkForkChoice(), awaitHeadTimeoutMs = 1200_000), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinZMQHead::class.java)
    }

    private var refreshSubscription: Disposable? = null

    fun connect(): Flux<BlockContainer> {
        return Flux.from(server.sink.asFlux())
            .onBackpressureLatest()
            .map {
                Hex.encodeHexString(it)
            }
            .flatMap { hash ->
                api.read(JsonRpcRequest("getblock", listOf(hash)))
                    .switchIfEmpty(Mono.error(IllegalStateException("Block $hash is not available on upstream")))
                    .retryWhen(Retry.backoff(5, Duration.ofMillis(100)))
                    .switchIfEmpty(Mono.fromCallable { log.warn("Block $hash is not available on upstream") }.then(Mono.empty()))
                    .flatMap(JsonRpcResponse::requireResult)
                    .map(extractBlock::extract)
                    .timeout(Defaults.timeout, Mono.error(Exception("Block data is not received")))
            }
            .onErrorResume { t ->
                log.warn("Failed to get a block from upstream with error: ${t.message}")
                connect()
            }
    }

    override fun start() {
        super.start()
        server.start()
        refreshSubscription = super.follow(connect())
    }

    override fun stop() {
        super.stop()
        server.stop()
        val copy = refreshSubscription
        refreshSubscription = null
        copy?.dispose()
    }

    override fun isRunning(): Boolean {
        return server.isRunning() || refreshSubscription != null
    }
}
