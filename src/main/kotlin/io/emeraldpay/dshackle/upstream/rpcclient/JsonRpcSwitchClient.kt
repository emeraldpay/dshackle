package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.reader.StandardRpcReader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Consumer

/**
 * An aggregating JSON RPC Client that wraps two actual readers, a Primary and a Secondary.
 * It always calls the Primary reader, and if it fails or produces an empty result, then it calls the Secondary reader.
 */
class JsonRpcSwitchClient(
    private val primary: StandardRpcReader,
    private val secondary: StandardRpcReader,
) : StandardRpcReader, WithHttpStatus {

    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcSwitchClient::class.java)
    }

    override var onHttpError: Consumer<Int>? = null

    init {
        if (primary is WithHttpStatus) {
            primary.onHttpError = Consumer { code ->
                onHttpError?.accept(code)
            }
        }
        if (secondary is WithHttpStatus) {
            secondary.onHttpError = Consumer { code ->
                onHttpError?.accept(code)
            }
        }
    }

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        return primary.read(key)
            .switchIfEmpty(Mono.error(IllegalStateException("No response from Primary Connection")))
            .onErrorResume {
                secondary.read(key)
            }
    }
}
