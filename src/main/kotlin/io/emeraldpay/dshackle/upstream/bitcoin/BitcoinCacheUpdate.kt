package io.emeraldpay.dshackle.upstream.bitcoin

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class BitcoinCacheUpdate(
    private val cache: Caches,
    private val delegate: StandardRpcReader,
) : StandardRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(BitcoinCacheUpdate::class.java)
    }

    private val extractBlock = ExtractBlock()

    override fun read(key: JsonRpcRequest): Mono<JsonRpcResponse> {
        return delegate.read(key)
            .doOnNext {
                if (it.hasResult()) {
                    try {
                        cacheResponse(key, it)
                    } catch (t: Throwable) {
                        log.warn("Failed to cache response: ${t.message}")
                    }
                }
            }
    }

    private fun cacheResponse(key: JsonRpcRequest, response: JsonRpcResponse) {
        when (key.method) {
            "getblock" -> {
                val block = extractBlock.extract(response.resultOrEmpty)
                cache.cache(Caches.Tag.REQUESTED, block)
            }
        }
    }
}
