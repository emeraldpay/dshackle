package io.emeraldpay.dshackle.upstream.rpcclient

import io.emeraldpay.dshackle.reader.Reader
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * An aggregating JSON RPC Client that wraps two actual readers, a Primary and a Secondary.
 * It always calls the Primary reader, and if it fails or produces an empty result, then it calls the Secondary reader.
 */
class JsonRpcSwitchClient(
    private val primary: Reader<JsonRpcRequest, JsonRpcResponse>,
    private val secondary: Reader<JsonRpcRequest, JsonRpcResponse>,
) : Reader<JsonRpcRequest, JsonRpcResponse> {

    companion object {
        private val log = LoggerFactory.getLogger(JsonRpcSwitchClient::class.java)
    }

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        return primary.read(key)
            .switchIfEmpty(Mono.error(IllegalStateException("No response from Primary Connection")))
            .onErrorResume {
                secondary.read(key)
            }
    }
}
