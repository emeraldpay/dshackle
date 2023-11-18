package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.BlockContainer
import io.emeraldpay.dshackle.data.DefaultContainer
import io.emeraldpay.dshackle.data.TxContainer
import io.emeraldpay.dshackle.data.TxId
import io.emeraldpay.dshackle.reader.StandardRpcReader
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcRequest
import io.emeraldpay.dshackle.upstream.rpcclient.JsonRpcResponse
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * Update cache with new data when it was requested from an upstream
 */
class CacheUpdate(
    private val caches: Caches,
    private val delegate: StandardRpcReader,
) : StandardRpcReader {

    companion object {
        private val log = LoggerFactory.getLogger(CacheUpdate::class.java)
    }

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
            "eth_getBlockByNumber", "eth_getBlockByHash" -> {
                val block = BlockContainer.fromEthereumJson(response.resultOrEmpty)
                caches.cache(Caches.Tag.REQUESTED, block)
            }
            "eth_getTransactionByHash" -> {
                val tx = TxContainer.fromEthereumJson(response.resultOrEmpty)
                caches.cache(Caches.Tag.REQUESTED, tx)
            }
            "eth_getTransactionReceipt" -> {
                val id = try {
                    TxId.from(key.params[0] as String)
                } catch (t: Throwable) {
                    return
                }
                val receipt = response.resultOrEmpty
                caches.cacheReceipt(
                    Caches.Tag.REQUESTED,
                    DefaultContainer(
                        txId = id,
                        json = receipt,
                    )
                )
            }
        }
    }
}
